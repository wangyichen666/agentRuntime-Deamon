//! 检索 worker 只计算；许可发布始终由 exact owner 的 repository 事务决定。
use super::*;
use agent_core::{ToolDiscoveryReceipt, ToolSearchIndex, ToolSearchQuery};
use sha2::{Digest, Sha256};
use std::sync::{Mutex, OnceLock};

type IndexCache = Mutex<Vec<(String, Arc<ToolSearchIndex>)>>;
static INDEX_CACHE: OnceLock<IndexCache> = OnceLock::new();
pub(super) const CORE_TOOLS: &[&str] = &[
    "read_file",
    "write_file",
    "edit_file",
    "exec",
    "plan",
    "sub_agent",
    "spawn_subagent",
];

pub(super) struct SearchCatalog {
    pub specs: Arc<Vec<ToolSpec>>,
    pub generation: String,
}
impl SearchCatalog {
    pub fn new(specs: Vec<ToolSpec>) -> Result<Self> {
        anyhow::ensure!(specs.len() <= 8192, "工具目录超过数量预算");
        anyhow::ensure!(
            specs.iter().all(|tool| !tool.name.is_empty()
                && tool.name.len() <= 256
                && !tool.name.chars().any(char::is_whitespace))
                && specs.windows(2).all(|pair| pair[0].name < pair[1].name),
            "冻结工具目录名称/顺序无效"
        );
        let bytes = serde_json::to_vec(&specs)?;
        anyhow::ensure!(bytes.len() <= 16 * 1024 * 1024, "工具目录超过字节预算");
        Ok(Self {
            generation: format!("{:x}", Sha256::digest(bytes)),
            specs: Arc::new(specs),
        })
    }
    pub async fn execute(
        &self,
        query: ToolSearchQuery,
        cancellation: &dyn ToolCancellation,
    ) -> Result<ToolOutput> {
        let (repository, owner) = crate::loop_engine::current_session_repository()
            .ok_or_else(|| anyhow::anyhow!("工具发现缺少 canonical owner"))?;
        let (_, call_id) = crate::loop_engine::current_tool_owner()
            .ok_or_else(|| anyhow::anyhow!("工具发现缺少执行身份"))?;
        let round = crate::loop_engine::current_tool_round()
            .ok_or_else(|| anyhow::anyhow!("工具发现缺执行轮次"))?;
        let generation = self.generation.clone();
        let specs = self.specs.clone();
        let input = query.clone();
        let worker = tokio::task::spawn_blocking(move || -> Result<_> {
            let index = cached_index(specs, generation)?;
            Ok((index.search(&input)?, index))
        });
        let (result, _index) = tokio::select! {
            result = worker => result??,
            _ = cancellation.cancelled() => anyhow::bail!("工具发现已取消"),
        };
        anyhow::ensure!(!cancellation.is_cancelled(), "工具发现已取消");
        let receipt = ToolDiscoveryReceipt {
            schema_version: 1,
            operation_id: format!("{}:discovery:{round}:{call_id}", owner.run_id.0),
            owner,
            query,
            result,
        };
        let saved = repository.publish_discovery(&receipt)?;
        Ok(ToolOutput::text(serde_json::to_string(&saved.result)?))
    }
}
fn cached_index(specs: Arc<Vec<ToolSpec>>, generation: String) -> Result<Arc<ToolSearchIndex>> {
    let cache = INDEX_CACHE.get_or_init(|| Mutex::new(Vec::new()));
    let mut cache = cache
        .lock()
        .map_err(|_| anyhow::anyhow!("索引缓存锁损坏"))?;
    if let Some(index) = cache
        .iter()
        .find_map(|(key, index)| (key == &generation).then(|| index.clone()))
    {
        return Ok(index);
    }
    let index = Arc::new(ToolSearchIndex::new((*specs).clone(), generation.clone())?);
    if cache.len() >= 16 {
        cache.remove(0);
    }
    cache.push((generation, index.clone()));
    Ok(index)
}

pub(super) fn provider_spec() -> ToolSpec {
    ToolSpec {
        name: "tool_search".into(),
        description: "在本次运行的冻结授权目录中发现工具。query 支持 select:精确名称、list 或中英文检索；发现后使用本工具的 {name,arguments} 调用，保留完整 schema 和原安全审批。".into(),
        parameters: serde_json::json!({"oneOf":[
            {"type":"object","properties":{"query":{"type":"string","minLength":1,"maxLength":2048},"limit":{"type":"integer","minimum":1,"maximum":8}},"required":["query"],"additionalProperties":false},
            {"type":"object","properties":{"name":{"type":"string"},"arguments":{}},"required":["name","arguments"],"additionalProperties":false}
        ]}),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    #[tokio::test(flavor = "multi_thread")]
    async fn cancelled_blocking_worker_cannot_publish_a_discovery_after_return() {
        use agent_core::*;
        use agent_storage::{DiscoveryRepository, RunStore, SessionLifecycle, SessionQuery};
        let store = Arc::new(RunStore::open(std::path::Path::new(":memory:")).unwrap());
        let meta = store
            .create_session(&SessionKey("cancel-search".into()))
            .unwrap();
        let catalog = Arc::new(
            SearchCatalog::new(vec![ToolSpec {
                name: "cancel__lookup".into(),
                description: "检索".into(),
                parameters: json!({"type":"object"}),
            }])
            .unwrap(),
        );
        let snapshot:RunSnapshot=serde_json::from_value(json!({"tools":*catalog.specs,"tool_catalog_digest":catalog.generation,"route":null,"cwd":".","permission_mode":"risk","sandbox_requested":"native","sandbox_effective":"native","context_read_only":false,"max_tool_calls":8,"config_generation":0})).unwrap();
        let Admission::New(run) = store
            .admit_run(
                &RunAdmission {
                    session_key: meta.key,
                    expected_lifetime: Some(meta.lifetime),
                    request_id: RequestId::Number(1),
                    input: "检索".into(),
                    mode: AdmissionMode::Queue,
                    plan_execution: None,
                },
                &snapshot,
            )
            .unwrap()
        else {
            panic!("new")
        };
        store.try_start_queued(&run.run_id).unwrap();
        let owner = store.run_owner(&run.run_id).unwrap();
        let cache = INDEX_CACHE.get_or_init(|| Mutex::new(Vec::new()));
        let (locked_tx, locked_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        let holder = std::thread::spawn(move || {
            let _guard = cache.lock().unwrap();
            locked_tx.send(()).unwrap();
            release_rx.blocking_recv().unwrap();
        });
        locked_rx.await.unwrap();
        struct Cancellation {
            token: crate::loop_engine::CancellationToken,
            waiting: tokio::sync::Notify,
        }
        #[async_trait]
        impl ToolCancellation for Cancellation {
            fn is_cancelled(&self) -> bool {
                self.token.is_cancelled()
            }
            async fn cancelled(&self) {
                self.waiting.notify_one();
                self.token.cancelled().await;
            }
        }
        let cancellation = Arc::new(Cancellation {
            token: crate::loop_engine::CancellationToken::new(),
            waiting: tokio::sync::Notify::new(),
        });
        let worker_store = store.clone();
        let worker_catalog = catalog.clone();
        let signal = cancellation.clone();
        let run_id = run.run_id.clone();
        let task = tokio::spawn(async move {
            crate::loop_engine::with_tool_audit(
                worker_store,
                run_id,
                crate::loop_engine::with_test_tool_identity(
                    "cancel-search-call".into(),
                    worker_catalog.execute(
                        ToolSearchQuery {
                            query: "list".into(),
                            limit: None,
                        },
                        &*signal,
                    ),
                ),
            )
            .await
        });
        cancellation.waiting.notified().await;
        store.request_cancellation(&owner).unwrap();
        cancellation.token.cancel();
        let result = tokio::time::timeout(std::time::Duration::from_secs(2), task)
            .await
            .unwrap()
            .unwrap();
        assert!(result.is_err());
        assert!(store.discoveries(&run.run_id).unwrap().is_empty());
        release_tx.send(()).unwrap();
        tokio::task::spawn_blocking(move || holder.join().unwrap())
            .await
            .unwrap();
        let late = cached_index(catalog.specs.clone(), catalog.generation.clone()).unwrap();
        assert_eq!(
            late.search(&ToolSearchQuery {
                query: "list".into(),
                limit: None
            })
            .unwrap()
            .tools
            .len(),
            1
        );
        assert!(store.discoveries(&run.run_id).unwrap().is_empty());
        assert!(
            store
                .events_after(&run.run_id, EventSeq(0), 100)
                .unwrap()
                .iter()
                .all(|event| event.event != "tool_discovered")
        );
    }
    #[tokio::test(flavor = "multi_thread")]
    async fn concurrent_queries_share_only_the_complete_snapshot_index_and_invalidate_changes() {
        let catalog = Arc::new(
            SearchCatalog::new(vec![ToolSpec {
                name: "cache__lookup".into(),
                description: "中文记忆 English lookup".into(),
                parameters: json!({"type":"object","properties":{"query":{"type":"string"}}}),
            }])
            .unwrap(),
        );
        let barrier = Arc::new(std::sync::Barrier::new(8));
        let workers = (0..8)
            .map(|_| {
                let barrier = barrier.clone();
                let catalog = catalog.clone();
                tokio::task::spawn_blocking(move || {
                    barrier.wait();
                    let index =
                        cached_index(catalog.specs.clone(), catalog.generation.clone()).unwrap();
                    assert_eq!(
                        index
                            .search(&ToolSearchQuery {
                                query: "记忆".into(),
                                limit: None
                            })
                            .unwrap()
                            .tools[0]
                            .name,
                        "cache__lookup"
                    );
                    index
                })
            })
            .collect::<Vec<_>>();
        let mut results = Vec::new();
        for worker in workers {
            results.push(worker.await.unwrap());
        }
        assert!(results.iter().all(|index| Arc::ptr_eq(index, &results[0])));
        for part in 0..3 {
            let mut specs = (*catalog.specs).clone();
            match part {
                0 => specs[0].description.push_str(" changed"),
                1 => specs[0].parameters = json!({"type":"string"}),
                _ => specs.clear(),
            }
            let changed = SearchCatalog::new(specs).unwrap();
            assert_ne!(changed.generation, catalog.generation);
            let index = cached_index(changed.specs, changed.generation).unwrap();
            assert!(!Arc::ptr_eq(&index, &results[0]));
            if part == 2 {
                assert!(
                    index
                        .search(&ToolSearchQuery {
                            query: "select:cache__lookup".into(),
                            limit: None
                        })
                        .is_err()
                );
            }
        }
        assert!(INDEX_CACHE.get().unwrap().lock().unwrap().len() <= 16);
    }
}
