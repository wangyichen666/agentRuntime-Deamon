//! 原事件与封存 stamp 的有界只读投影；不创建第二份事件事实。
use crate::{RunStore, RuntimeError, sessions, views};
use agent_core::*;
use rusqlite::{Connection, OptionalExtension, params};

// 小于 JSON-RPC frame 上限，留出 envelope/分页元数据空间。
pub(crate) const EVENT_PAGE_BYTES: usize = 1024 * 1024 - 1024;

pub(crate) fn is_live_event(event: &str) -> bool {
    agent_core::is_run_live_event(event)
}

fn decode_event_in(
    db: &Connection,
    run: &RunId,
    seq: u64,
    event: String,
    raw: &str,
) -> Result<StoredEvent, RuntimeError> {
    let mut data: serde_json::Value =
        serde_json::from_str(raw).map_err(|error| RuntimeError::Protocol(error.to_string()))?;
    views::strip_transport_metadata(&mut data);
    if let Some(stamp) = views::stamp_in(db, run, EventSeq(seq))? {
        data.as_object_mut()
            .ok_or_else(|| RuntimeError::Protocol("事件 data 非 object".into()))?
            .insert(
                "_my_agent_view".into(),
                serde_json::to_value(stamp)
                    .map_err(|error| RuntimeError::Protocol(error.to_string()))?,
            );
    }
    Ok(StoredEvent {
        run_id: run.clone(),
        seq: EventSeq(seq),
        event,
        data,
    })
}

// 原维护/事务调用与在线端口共用 SQL 和 stamp 解码；预算只改变读回窗口。
pub(crate) fn events_in(
    db: &Connection,
    run: &RunId,
    after: EventSeq,
    limit: usize,
    byte_budget: Option<usize>,
) -> Result<RunEventPage, RuntimeError> {
    let mut statement = db.prepare("SELECT seq,event,CASE WHEN ?4 IS NULL OR length(CAST(data_json AS BLOB))<=?4 THEN data_json END FROM events WHERE run_id=?1 AND seq>?2 ORDER BY seq LIMIT ?3")?;
    let fetch = if byte_budget.is_some() {
        limit + 1
    } else {
        limit
    };
    let mut rows = statement.query(params![
        run.0,
        after.0,
        fetch as i64,
        byte_budget.map(|v| v as i64)
    ])?;
    let mut page = RunEventPage {
        events: Vec::new(),
        cursor: after,
        has_more: false,
    };
    let mut bytes = 2usize;
    while let Some(row) = rows.next()? {
        if page.events.len() == limit {
            page.has_more = true;
            break;
        }
        let raw: Option<String> = row.get(2)?;
        let Some(raw) = raw else {
            if page.events.is_empty() {
                return Err(CoordinationError::InvalidRequest.into());
            }
            page.has_more = true;
            break;
        };
        let event = decode_event_in(db, run, row.get(0)?, row.get(1)?, &raw)?;
        if let Some(budget) = byte_budget {
            let size = serde_json::to_vec(&event)
                .map_err(|error| RuntimeError::Protocol(error.to_string()))?
                .len()
                + 1;
            if size > budget.saturating_sub(bytes) {
                if page.events.is_empty() {
                    return Err(CoordinationError::InvalidRequest.into());
                }
                page.has_more = true;
                break;
            }
            bytes += size;
        }
        page.cursor = event.seq;
        page.events.push(event);
    }
    Ok(page)
}

// 最新事件仍使用原分页解码器；原同步helper和async caller共用这一事务体。
impl RunStore {
    pub(crate) fn view_event_scoped(
        &self,
        scope: &TenantContext,
        run: &RunId,
        event: &str,
        child: Option<&RunId>,
    ) -> Result<Option<StoredEvent>, RuntimeError> {
        let mut connection = self.lock_connection()?;
        let tx = connection.transaction()?;
        crate::enterprise::check_run_scope_in(&tx, scope, run)?;
        let owner = sessions::owner_in(&tx, run)?;
        sessions::lifetime_owner_in(&tx, &owner.session_key, &owner.session_lifetime_id)?;
        let row:Option<(u64,Option<String>)>=tx.query_row("SELECT seq,CASE WHEN length(CAST(data_json AS BLOB))<=?4 THEN data_json END FROM events WHERE run_id=?1 AND event=?2 AND (?3 IS NULL OR CASE WHEN length(CAST(data_json AS BLOB))>?4 THEN 1 WHEN json_valid(data_json)=0 THEN 1 ELSE json_extract(data_json,'$.child_run_id')=?3 END) ORDER BY seq DESC LIMIT 1",params![run.0,event,child.map(|child|&child.0),EVENT_PAGE_BYTES as u64],|row|Ok((row.get(0)?,row.get(1)?))).optional()?;
        let found = row
            .map(|(seq, raw)| -> Result<StoredEvent, RuntimeError> {
                let raw = raw.ok_or(CoordinationError::InvalidRequest)?;
                let event = decode_event_in(&tx, run, seq, event.to_owned(), &raw)?;
                if serde_json::to_vec(&event)
                    .map_err(|_| CoordinationError::Corrupt)?
                    .len()
                    > EVENT_PAGE_BYTES
                {
                    return Err(CoordinationError::InvalidRequest.into());
                }
                Ok(event)
            })
            .transpose()?;
        tx.commit()?;
        Ok(found)
    }
}

impl RunStore {
    fn event_page(
        &self,
        scope: &TenantContext,
        run: &RunId,
        after: EventSeq,
        limit: u32,
    ) -> Result<RunEventPage, RuntimeError> {
        let mut db = self.lock_connection()?;
        let tx = db.transaction()?;
        // 不存在与越域同一错误；scope 判定在 SQL 内，先于读取 payload/stamp。
        crate::enterprise::check_run_scope_in(&tx, scope, run)?;
        let owner = sessions::owner_in(&tx, run)?;
        sessions::lifetime_owner_in(&tx, &owner.session_key, &owner.session_lifetime_id)?;
        // 不取得 writer proof；已退休 run 的 immutable 历史仍可检查。
        let page = events_in(&tx, run, after, limit as usize, Some(EVENT_PAGE_BYTES))?;
        tx.commit()?;
        Ok(page)
    }
}

#[async_trait::async_trait]
impl agent_storage_ports::RunEventRepository for RunStore {
    async fn append_run_live_event(
        &self,
        envelope: &EnterpriseOwnerEnvelope,
        event: &str,
        data: &serde_json::Value,
    ) -> Result<EventSeq, RepositoryError> {
        if !crate::execution::valid_owner(&envelope.owner)
            || !is_live_event(event)
            || !data.is_object()
            || !crate::execution::within_json_budget(data, EVENT_PAGE_BYTES - 16 * 1024)
            || (event == "approval_required"
                && data
                    .get("approval")
                    .and_then(|approval| approval.get("id"))
                    .and_then(serde_json::Value::as_str)
                    .is_none_or(|id| id.is_empty() || id.len() > 1024))
        {
            return Err(CoordinationError::InvalidRequest.into());
        }
        let (envelope, event, data) = (envelope.clone(), event.to_owned(), data.clone());
        self.sqlite_blocking(CoordinationError::StorageUnavailable.into(), move |store| {
            store
                .append_event_scoped(&envelope.owner.run_id, &event, &data, Some(&envelope))
                .map_err(crate::execution::execution_error)
        })
        .await
    }

    async fn read_run_view_event(
        &self,
        scope: &TenantContext,
        run: &RunId,
        event: &str,
        child: Option<&RunId>,
    ) -> Result<Option<StoredEvent>, CoordinationError> {
        if run.0.is_empty()
            || run.0.len() > 1024
            || event.is_empty()
            || event.len() > 128
            || child.is_some_and(|child| child.0.is_empty() || child.0.len() > 1024)
        {
            return Err(CoordinationError::InvalidRequest);
        }
        let (scope, run, event, child) =
            (scope.clone(), run.clone(), event.to_owned(), child.cloned());
        self.sqlite_blocking(CoordinationError::StorageUnavailable, move |store| {
            store
                .view_event_scoped(&scope, &run, &event, child.as_ref())
                .map_err(crate::readback::read_error)
        })
        .await
    }

    async fn read_run_events(
        &self,
        scope: &TenantContext,
        run: &RunId,
        after: EventSeq,
        limit: u32,
    ) -> Result<RunEventPage, CoordinationError> {
        if run.0.is_empty()
            || run.0.len() > 1024
            || after.0 > i64::MAX as u64
            || !(1..=1000).contains(&limit)
        {
            return Err(CoordinationError::InvalidRequest);
        }
        let (scope, run) = (scope.clone(), run.clone());
        self.sqlite_blocking(CoordinationError::StorageUnavailable, move |store| {
            store
                .event_page(&scope, &run, after, limit)
                .map_err(crate::readback::read_error)
        })
        .await
    }
}

#[cfg(test)]
mod async_contract {
    use super::*;
    use agent_storage_ports::RunEventRepository;

    #[tokio::test(flavor = "current_thread")]
    async fn event_readback_yields_during_real_connection_contention() {
        let root = std::env::temp_dir().join(format!("ma-event-adapter-{}", std::process::id()));
        let store = RunStore::open(&root.join("runtime.sqlite3")).unwrap();
        let Admission::New(run) = store
            .admit(SessionKey("events".into()), RequestId::Number(1), "事件")
            .unwrap()
        else {
            panic!("新准入")
        };
        let occupied = store.clone();
        let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = std::sync::mpsc::sync_channel(1);
        let holder = std::thread::spawn(move || {
            let db = occupied.lock_connection().unwrap();
            db.query_row("SELECT count(*) FROM events", [], |r| r.get::<_, u64>(0))
                .unwrap();
            ready_tx.send(()).unwrap();
            let _ = release_rx.recv_timeout(std::time::Duration::from_secs(5));
            drop(db);
        });
        ready_rx.await.unwrap();
        let result = tokio::time::timeout(
            std::time::Duration::from_millis(100),
            store.read_run_events(&TenantContext::local(), &run.run_id, EventSeq(0), 1),
        )
        .await;
        let _ = release_tx.send(());
        holder.join().unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            while store.blocking_budget.available_permits() != 4 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(
            store
                .read_run_events(&TenantContext::local(), &run.run_id, EventSeq(0), 1)
                .await
                .is_ok()
        );
        drop(store);
        std::fs::remove_dir_all(root).unwrap();
        assert!(
            result.is_err(),
            "事件读回不能占有 Tokio 线程等原连接：{result:?}"
        );
    }
    #[tokio::test(flavor = "current_thread")]
    async fn latest_event_readback_yields_during_real_connection_contention() {
        let root =
            std::env::temp_dir().join(format!("ma-event-view-adapter-{}", std::process::id()));
        let store = RunStore::open(&root.join("runtime.sqlite3")).unwrap();
        let Admission::New(run) = store
            .admit(SessionKey("events".into()), RequestId::Number(1), "事件")
            .unwrap()
        else {
            panic!("新准入")
        };
        let occupied = store.clone();
        let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = std::sync::mpsc::sync_channel(1);
        let holder = std::thread::spawn(move || {
            let db = occupied.lock_connection().unwrap();
            ready_tx.send(()).unwrap();
            let _ = release_rx.recv_timeout(std::time::Duration::from_secs(5));
            drop(db);
        });
        ready_rx.await.unwrap();
        let now = std::time::Instant::now();
        let result = tokio::time::timeout(
            std::time::Duration::from_millis(25),
            store.read_run_view_event(&TenantContext::local(), &run.run_id, "terminal", None),
        )
        .await;
        let elapsed = now.elapsed();
        let _ = release_tx.send(());
        holder.join().unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            while store.blocking_budget.available_permits() != 4 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(result.is_err());
        assert!(
            elapsed < std::time::Duration::from_millis(150),
            "timer被DB锁阻塞：{elapsed:?}"
        );
        assert!(
            store
                .read_run_view_event(&TenantContext::local(), &run.run_id, "terminal", None)
                .await
                .unwrap()
                .is_none()
        );
        drop(store);
        std::fs::remove_dir_all(root).unwrap();
    }
    #[tokio::test(flavor = "current_thread")]
    async fn live_event_cancelled_awaiter_keeps_worker_until_one_real_commit() {
        let root =
            std::env::temp_dir().join(format!("ma-live-event-worker-{}", std::process::id()));
        let store = RunStore::open(&root.join("runtime.sqlite3")).unwrap();
        let Admission::New(run) = store
            .admit(
                SessionKey("live-events".into()),
                RequestId::Number(1),
                "开始",
            )
            .unwrap()
        else {
            panic!("新准入")
        };
        assert!(store.try_start_queued(&run.run_id).unwrap());
        let env = EnterpriseOwnerEnvelope {
            scope: TenantContext::local(),
            owner: sessions::owner_in(&store.lock_connection().unwrap(), &run.run_id).unwrap(),
        };
        let before: u64 = store
            .lock_connection()
            .unwrap()
            .query_row(
                "SELECT count(*) FROM events WHERE run_id=?1",
                [&run.run_id.0],
                |r| r.get(0),
            )
            .unwrap();
        let occupied = store.clone();
        let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = std::sync::mpsc::sync_channel(1);
        let holder = std::thread::spawn(move || {
            let db = occupied.lock_connection().unwrap();
            ready_tx.send(()).unwrap();
            let _ = release_rx.recv_timeout(std::time::Duration::from_secs(5));
            drop(db);
        });
        ready_rx.await.unwrap();
        let started = std::time::Instant::now();
        let result = tokio::time::timeout(
            std::time::Duration::from_millis(25),
            store.append_run_live_event(
                &env,
                "text_delta",
                &serde_json::json!({"delta":"实际提交"}),
            ),
        )
        .await;
        let elapsed = started.elapsed();
        let budget = store.blocking_budget.available_permits();
        let _ = release_tx.send(());
        holder.join().unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            while store.blocking_budget.available_permits() != 4 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let after: u64 = store
            .lock_connection()
            .unwrap()
            .query_row(
                "SELECT count(*) FROM events WHERE run_id=?1",
                [&run.run_id.0],
                |r| r.get(0),
            )
            .unwrap();
        assert!(result.is_err());
        assert!(
            elapsed < std::time::Duration::from_millis(150),
            "SQLite连接等待阻塞Tokio timer: {elapsed:?}"
        );
        assert_eq!(budget, 3, "awaiter取消后实际worker继续占有原permit");
        assert_eq!(after, before + 1, "awaiter取消后已启动写入恰好提交一次");
        drop(store);
        std::fs::remove_dir_all(root).unwrap();
    }
}
