//! 文件 artifact 的真实路径边界与原 tool receipt；不以路径字符串检查替代句柄。
use agent_storage::RunStore;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
static NEXT: AtomicU64 = AtomicU64::new(0);
fn root() -> PathBuf {
    let path = std::env::temp_dir().join(format!(
        "ma-artifact-boundary-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir_all(&path).unwrap();
    path
}
#[test]
fn artifact_directory_symlink_cannot_redirect_publication_to_another_directory() {
    let root = root();
    let path = root.join("runtime.sqlite3");
    let outside = root.join("outside");
    std::fs::create_dir(&outside).unwrap();
    let store = RunStore::open(&path).unwrap();
    std::os::unix::fs::symlink(&outside, path.with_extension("artifacts")).unwrap();
    assert!(
        store.store_tool_output("固定输出").is_err(),
        "artifact 根 symlink 必须在任何写入前拒绝"
    );
    assert!(std::fs::read_dir(&outside).unwrap().next().is_none());
    drop(store);
    std::fs::remove_dir_all(root).unwrap();
}

use agent_core::*;
use agent_storage::{ArtifactRepository, MaintenanceRepository, RuntimeError, SessionQuery};
use serde_json::json;
use sha2::{Digest, Sha256};
fn owned(store: &RunStore, body: &str) -> (ExactOwner, String) {
    let Admission::New(run) = store
        .admit_with_route(
            SessionKey("artifact-owner".into()),
            RequestId::Number(1),
            "存储测试",
            AdmissionMode::Queue,
            None,
        )
        .unwrap()
    else {
        panic!("新 run")
    };
    store.try_start_queued(&run.run_id).unwrap();
    let owner = store.run_owner(&run.run_id).unwrap();
    let call = ToolCall {
        id: "artifact-call".into(),
        name: "read".into(),
        arguments: json!({}),
    };
    store
        .prepare_tool_batch(
            &run.run_id,
            1,
            &[(call, "read_only".into(), "digest".into(), false)],
        )
        .unwrap();
    store.start_tool(&run.run_id, 1, "artifact-call").unwrap();
    let reference = store.store_owned_output(&owner, body).unwrap();
    store
        .finish_tool(
            &run.run_id,
            1,
            "artifact-call",
            "success",
            Some(&reference),
            &json!({"output_sha256":format!("{:x}",Sha256::digest(body.as_bytes())),"output_bytes":body.len()}),
        )
        .unwrap();
    (owner, reference)
}
#[test]
fn artifact_existing_symlink_and_hardlink_are_rejected_without_changing_outside_content() {
    let root = root();
    let path = root.join("runtime.sqlite3");
    let store = RunStore::open(&path).unwrap();
    let reference = store.store_tool_output("相同内容").unwrap();
    std::fs::remove_file(&reference).unwrap();
    let outside = root.join("outside.txt");
    std::fs::write(&outside, "相同内容").unwrap();
    std::os::unix::fs::symlink(&outside, &reference).unwrap();
    assert!(matches!(
        store.store_tool_output("相同内容"),
        Err(RuntimeError::Artifact(ArtifactError::Forbidden))
    ));
    std::fs::remove_file(&reference).unwrap();
    std::fs::hard_link(&outside, &reference).unwrap();
    assert!(matches!(
        store.store_tool_output("相同内容"),
        Err(RuntimeError::Artifact(ArtifactError::Forbidden))
    ));
    assert_eq!(std::fs::read_to_string(outside).unwrap(), "相同内容");
    drop(store);
    std::fs::remove_dir_all(root).unwrap();
}
#[test]
fn artifact_bound_directory_replacement_rejects_read_publish_and_gc() {
    let root = root();
    let path = root.join("runtime.sqlite3");
    let store = RunStore::open(&path).unwrap();
    let (owner, reference) = owned(&store, "绑定的内容");
    let directory = path.with_extension("artifacts");
    let old = root.join("old-artifacts");
    std::fs::rename(&directory, &old).unwrap();
    std::fs::create_dir(&directory).unwrap();
    assert!(matches!(
        store.read_run_artifact(&owner, &reference, 0, 16),
        Err(RuntimeError::Artifact(ArtifactError::Conflict))
    ));
    assert!(matches!(
        store.store_owned_output(&owner, "新内容"),
        Err(RuntimeError::Artifact(ArtifactError::Conflict))
    ));
    assert!(matches!(
        store.gc_artifacts(std::time::Duration::ZERO),
        Err(RuntimeError::Artifact(ArtifactError::Conflict))
    ));
    assert!(std::fs::read_dir(&directory).unwrap().next().is_none());
    assert_eq!(std::fs::read_dir(&old).unwrap().count(), 1);
    drop(store);
    std::fs::remove_dir_all(root).unwrap();
}
#[test]
fn artifact_reference_receipt_restart_pagination_and_corruption_share_original_owner() {
    let root = root();
    let path = root.join("runtime.sqlite3");
    let store = RunStore::open(&path).unwrap();
    let (owner, reference) = owned(&store, "abcdef中文");
    assert_eq!(
        store.read_run_artifact(&owner, &reference, 1, 4).unwrap(),
        b"bcde"
    );
    let mut foreign = owner.clone();
    foreign.turn_id = TurnId("different-turn".into());
    assert!(store.read_run_artifact(&foreign, &reference, 0, 4).is_err());
    let mut foreignref = reference.clone();
    foreignref.push_str(".outside");
    assert!(matches!(
        store.read_run_artifact(&owner, &foreignref, 0, 4),
        Err(RuntimeError::Artifact(ArtifactError::Forbidden))
    ));
    drop(store);
    let store = RunStore::open(&path).unwrap();
    assert_eq!(
        store.read_run_artifact(&owner, &reference, 6, 6).unwrap(),
        "中文".as_bytes()
    );
    std::fs::write(&reference, "篡改").unwrap();
    assert!(matches!(
        store.read_run_artifact(&owner, &reference, 0, 4),
        Err(RuntimeError::Artifact(ArtifactError::Corrupt))
    ));
    drop(store);
    std::fs::remove_dir_all(root).unwrap();
}
#[test]
fn artifact_concurrent_publishers_never_replace_existing_verified_object() {
    let root = root();
    let path = root.join("runtime.sqlite3");
    let first = RunStore::open(&path).unwrap();
    let second = RunStore::open(&path).unwrap();
    let barrier = std::sync::Arc::new(std::sync::Barrier::new(3));
    let handles = [first, second]
        .into_iter()
        .map(|store| {
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                barrier.wait();
                store.store_tool_output(&"内容".repeat(10000))
            })
        })
        .collect::<Vec<_>>();
    barrier.wait();
    let a = handles
        .into_iter()
        .map(|h| h.join().unwrap().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(a[0], a[1]);
    assert_eq!(
        std::fs::read_to_string(&a[0]).unwrap(),
        "内容".repeat(10000)
    );
    assert_eq!(
        std::fs::read_dir(path.with_extension("artifacts"))
            .unwrap()
            .count(),
        1
    );
    std::fs::remove_dir_all(root).unwrap();
}
#[test]
fn artifact_parent_symlink_is_rejected_before_database_creation_or_migration() {
    let root = root();
    let outside = root.join("outside");
    std::fs::create_dir(&outside).unwrap();
    let link = root.join("linked-parent");
    std::os::unix::fs::symlink(&outside, &link).unwrap();
    assert!(matches!(
        RunStore::open(&link.join("runtime.sqlite3")),
        Err(RuntimeError::Artifact(ArtifactError::Forbidden))
    ));
    assert!(std::fs::read_dir(&outside).unwrap().next().is_none());
    std::fs::remove_dir_all(root).unwrap();
}
#[test]
fn artifact_gc_never_deletes_external_links_or_referenced_objects() {
    let root = root();
    let path = root.join("runtime.sqlite3");
    let store = RunStore::open(&path).unwrap();
    let (owner, reference) = owned(&store, "应保留");
    let garbage = store.store_tool_output("垃圾").unwrap();
    let outside = root.join("outside.txt");
    std::fs::write(&outside, "外部").unwrap();
    let malicious = store.store_tool_output("外部").unwrap();
    std::fs::remove_file(&malicious).unwrap();
    std::os::unix::fs::symlink(&outside, &malicious).unwrap();
    assert!(
        store.gc_artifacts(std::time::Duration::ZERO).is_err(),
        "不将链接当作可安全清理的普通对象"
    );
    assert_eq!(std::fs::read_to_string(&outside).unwrap(), "外部");
    assert_eq!(
        store.read_run_artifact(&owner, &reference, 0, 16).unwrap(),
        "应保留".as_bytes()
    );
    std::fs::remove_file(malicious).unwrap();
    let _ = store.gc_artifacts(std::time::Duration::ZERO).unwrap();
    assert!(!std::path::Path::new(&garbage).exists());
    assert!(std::path::Path::new(&reference).exists());
    drop(store);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn artifact_database_digest_or_size_mismatch_fails_closed_without_returning_bytes() {
    let root = root();
    let path = root.join("runtime.sqlite3");
    let store = RunStore::open(&path).unwrap();
    let (owner, reference) = owned(&store, "数据库与对象必须一致");
    let db = rusqlite::Connection::open(&path).unwrap();
    let original: String = db
        .query_row("SELECT receipt_json FROM tool_executions", [], |r| r.get(0))
        .unwrap();
    for field in ["output_sha256", "output_bytes"] {
        let mut receipt: serde_json::Value = serde_json::from_str(&original).unwrap();
        receipt[field] = if field == "output_sha256" {
            json!("0".repeat(64))
        } else {
            json!(1)
        };
        db.execute(
            "UPDATE tool_executions SET receipt_json=?1",
            [receipt.to_string()],
        )
        .unwrap();
        assert!(matches!(
            store.read_run_artifact(&owner, &reference, 0, 4),
            Err(RuntimeError::Artifact(ArtifactError::Corrupt))
        ));
    }
    db.execute("UPDATE tool_executions SET receipt_json=?1", [original])
        .unwrap();
    assert!(
        !store
            .read_run_artifact(&owner, &reference, 0, 4)
            .unwrap()
            .is_empty()
    );
    drop(db);
    drop(store);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn artifact_invalid_database_reference_cannot_commit_a_terminal_tool_receipt() {
    let root = root();
    let path = root.join("runtime.sqlite3");
    let store = RunStore::open(&path).unwrap();
    let Admission::New(run) = store
        .admit_with_route(
            SessionKey("bad-publication".into()),
            RequestId::Number(1),
            "发布边界",
            AdmissionMode::Queue,
            None,
        )
        .unwrap()
    else {
        panic!("新 run")
    };
    store.try_start_queued(&run.run_id).unwrap();
    let call = ToolCall {
        id: "bad-call".into(),
        name: "read".into(),
        arguments: json!({}),
    };
    store
        .prepare_tool_batch(
            &run.run_id,
            1,
            &[(call, "read_only".into(), "digest".into(), false)],
        )
        .unwrap();
    store.start_tool(&run.run_id, 1, "bad-call").unwrap();
    let reference = store.store_tool_output("正确对象").unwrap();
    let result = store.finish_tool(
        &run.run_id,
        1,
        "bad-call",
        "success",
        Some(&reference),
        &json!({"output_sha256":"0".repeat(64),"output_bytes":1}),
    );
    assert!(
        result.is_err(),
        "摘要/大小不匹配时，不得发布原 tool terminal/ref"
    );
    let receipt = store.tool_receipts(&run.run_id).unwrap().remove(0);
    assert_eq!(receipt.status, "running");
    assert!(receipt.artifact_ref.is_none());
    drop(store);
    std::fs::remove_dir_all(root).unwrap();
}

fn running(store: &RunStore) -> ExactOwner {
    let Admission::New(run) = store
        .admit_with_route(
            SessionKey("atomic-artifact".into()),
            RequestId::Number(1),
            "发布事务",
            AdmissionMode::Queue,
            None,
        )
        .unwrap()
    else {
        panic!("新 run")
    };
    store.try_start_queued(&run.run_id).unwrap();
    store
        .prepare_tool_batch(
            &run.run_id,
            1,
            &[(
                ToolCall {
                    id: "atomic-call".into(),
                    name: "read".into(),
                    arguments: json!({}),
                },
                "read_only".into(),
                "digest".into(),
                false,
            )],
        )
        .unwrap();
    store.start_tool(&run.run_id, 1, "atomic-call").unwrap();
    store.run_owner(&run.run_id).unwrap()
}
fn completion(body: &str) -> ToolArtifactCompletion {
    ToolArtifactCompletion {
        round: 1,
        call_id: "atomic-call".into(),
        outcome: "success".into(),
        receipt: json!({"output_sha256":format!("{:x}",Sha256::digest(body.as_bytes())),"output_bytes":body.len(),"output":body,"success":true}),
    }
}
#[test]
fn artifact_two_phase_receipt_is_idempotent_and_restarts_without_republishing_effects() {
    let root = root();
    let path = root.join("runtime.sqlite3");
    let store = RunStore::open(&path).unwrap();
    let owner = running(&store);
    let body = "新路径输出";
    let request = completion(body);
    let reference = store
        .finish_owned_tool_output(&owner, &request, body)
        .unwrap();
    let original = serde_json::to_value(store.tool_receipts(&owner.run_id).unwrap()).unwrap();
    let clock = store
        .session_readback(&owner.session_key, HistoryReadMode::Omitted)
        .unwrap()
        .snapshot_revision;
    assert_eq!(
        store
            .finish_owned_tool_output(&owner, &request, body)
            .unwrap(),
        reference
    );
    let changed = completion("不同 payload");
    assert!(matches!(
        store.finish_owned_tool_output(&owner, &changed, "不同 payload"),
        Err(RuntimeError::Artifact(ArtifactError::Conflict))
    ));
    assert_eq!(store.gc_artifacts(std::time::Duration::ZERO).unwrap(), 0);
    assert_eq!(
        store
            .session_readback(&owner.session_key, HistoryReadMode::Omitted)
            .unwrap()
            .snapshot_revision,
        clock
    );
    assert_eq!(
        std::fs::read_dir(path.with_extension("artifacts"))
            .unwrap()
            .count(),
        1
    );
    drop(store);
    let store = RunStore::open(&path).unwrap();
    assert_eq!(
        serde_json::to_value(store.tool_receipts(&owner.run_id).unwrap()).unwrap(),
        original
    );
    assert_eq!(
        store
            .read_run_artifact(&owner, &reference, 0, 16384)
            .unwrap(),
        body.as_bytes()
    );
    drop(store);
    std::fs::remove_dir_all(root).unwrap();
}
#[test]
fn artifact_two_phase_database_failure_rolls_back_original_receipt_and_gc_cleans_orphan() {
    let root = root();
    let path = root.join("runtime.sqlite3");
    let store = RunStore::open(&path).unwrap();
    let owner = running(&store);
    let db = rusqlite::Connection::open(&path).unwrap();
    db.execute_batch("CREATE TRIGGER fail_artifact_receipt BEFORE UPDATE ON tool_executions WHEN NEW.status='terminal' BEGIN SELECT RAISE(ABORT,'收据故障'); END;").unwrap();
    let before = serde_json::to_value(store.tool_receipts(&owner.run_id).unwrap()).unwrap();
    let body = "已落盘未提交";
    assert!(
        store
            .finish_owned_tool_output(&owner, &completion(body), body)
            .is_err()
    );
    assert_eq!(
        serde_json::to_value(store.tool_receipts(&owner.run_id).unwrap()).unwrap(),
        before
    );
    let directory = path.with_extension("artifacts");
    assert_eq!(
        std::fs::read_dir(&directory).unwrap().count(),
        1,
        "只剩未引用对象，无临时对象"
    );
    drop(store);
    let store = RunStore::open(&path).unwrap();
    assert_eq!(
        serde_json::to_value(store.tool_receipts(&owner.run_id).unwrap()).unwrap(),
        before
    );
    assert_eq!(store.gc_artifacts(std::time::Duration::ZERO).unwrap(), 1);
    assert!(std::fs::read_dir(directory).unwrap().next().is_none());
    drop(db);
    drop(store);
    std::fs::remove_dir_all(root).unwrap();
}
#[test]
fn artifact_two_phase_bad_digest_or_stale_owner_creates_no_published_object() {
    let root = root();
    let path = root.join("runtime.sqlite3");
    let store = RunStore::open(&path).unwrap();
    let owner = running(&store);
    let body = "原输出";
    let mut bad = completion(body);
    bad.receipt["output_sha256"] = json!("0".repeat(64));
    assert!(matches!(
        store.finish_owned_tool_output(&owner, &bad, body),
        Err(RuntimeError::Artifact(ArtifactError::Corrupt))
    ));
    assert!(
        std::fs::read_dir(path.with_extension("artifacts"))
            .unwrap()
            .next()
            .is_none()
    );
    let mut stale = owner.clone();
    stale.turn_id = TurnId("旧 turn".into());
    assert!(
        store
            .finish_owned_tool_output(&stale, &completion(body), body)
            .is_err()
    );
    assert!(
        std::fs::read_dir(path.with_extension("artifacts"))
            .unwrap()
            .next()
            .is_none()
    );
    assert_eq!(
        store.tool_receipts(&owner.run_id).unwrap()[0].status,
        "running"
    );
    drop(store);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn artifact_crash_temporary_cleanup_keeps_recent_stages_and_unrecognized_files() {
    let root = root();
    let path = root.join("runtime.sqlite3");
    let store = RunStore::open(&path).unwrap();
    let (_, reference) = owned(&store, "已发布证据");
    let directory = path.with_extension("artifacts");
    let old = directory.join(format!(".{}-123-1.tmp", "0".repeat(64)));
    let recent = directory.join(format!(".{}-123-2.tmp", "1".repeat(64)));
    let unrelated = directory.join("not-an-artifact.tmp");
    std::fs::write(&old, "崩溃后的部分临时输出").unwrap();
    std::fs::write(&recent, "活动临时输出").unwrap();
    std::fs::write(&unrelated, "未知文件").unwrap();
    let oldtime = std::time::SystemTime::now() - std::time::Duration::from_secs(172800);
    std::fs::File::options()
        .write(true)
        .open(&old)
        .unwrap()
        .set_times(std::fs::FileTimes::new().set_modified(oldtime))
        .unwrap();
    drop(store);
    let store = RunStore::open(&path).unwrap();
    assert_eq!(store.gc_artifacts(std::time::Duration::ZERO).unwrap(), 1);
    assert!(!old.exists());
    assert!(recent.exists());
    assert!(unrelated.exists());
    assert!(std::path::Path::new(&reference).exists());
    drop(store);
    std::fs::remove_dir_all(root).unwrap();
}
