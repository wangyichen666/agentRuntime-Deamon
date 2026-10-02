//! Wave 0 静态护栏：检查实际生产 AST，测试 fixture 不形成生产依赖。
use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use syn::visit::{self, Visit};
use syn::{Attribute, Item, ItemStruct, UseTree};

fn test_only(attrs: &[Attribute]) -> bool {
    attrs.iter().any(|attr| {
        attr.path().is_ident("test")
            || (attr.path().is_ident("cfg")
                && attr
                    .parse_args::<syn::Path>()
                    .is_ok_and(|path| path.is_ident("test")))
    })
}

#[derive(Default)]
struct Production {
    paths: Vec<Vec<String>>,
    fields: Vec<(String, String, BTreeSet<String>)>,
    definitions: BTreeSet<String>,
}

fn use_paths(tree: &UseTree, prefix: &[String], output: &mut Vec<Vec<String>>) {
    match tree {
        UseTree::Path(path) => {
            let mut next = prefix.to_vec();
            next.push(path.ident.to_string());
            use_paths(&path.tree, &next, output);
        }
        UseTree::Group(group) => {
            for item in &group.items {
                use_paths(item, prefix, output);
            }
        }
        UseTree::Name(name) => {
            let mut path = prefix.to_vec();
            path.push(name.ident.to_string());
            output.push(path);
        }
        UseTree::Rename(rename) => {
            let mut path = prefix.to_vec();
            path.push(rename.ident.to_string());
            output.push(path);
        }
        UseTree::Glob(_) => output.push(prefix.to_vec()),
    }
}

impl<'ast> Visit<'ast> for Production {
    fn visit_item(&mut self, item: &'ast Item) {
        let attrs: &[Attribute] = match item {
            Item::Mod(i) => &i.attrs,
            Item::Fn(i) => &i.attrs,
            Item::Struct(i) => &i.attrs,
            Item::Enum(i) => &i.attrs,
            Item::Impl(i) => &i.attrs,
            Item::Use(i) => &i.attrs,
            Item::Const(i) => &i.attrs,
            Item::Static(i) => &i.attrs,
            Item::Trait(i) => &i.attrs,
            Item::Type(i) => &i.attrs,
            Item::Macro(i) => &i.attrs,
            _ => &[],
        };
        if !test_only(attrs) {
            visit::visit_item(self, item);
        }
    }

    fn visit_item_use(&mut self, item: &'ast syn::ItemUse) {
        use_paths(&item.tree, &[], &mut self.paths);
    }

    fn visit_path(&mut self, path: &'ast syn::Path) {
        self.paths
            .push(path.segments.iter().map(|s| s.ident.to_string()).collect());
        visit::visit_path(self, path);
    }

    fn visit_item_struct(&mut self, item: &'ast ItemStruct) {
        self.definitions.insert(item.ident.to_string());
        for field in &item.fields {
            if test_only(&field.attrs) {
                continue;
            }
            let mut types = Production::default();
            types.visit_type(&field.ty);
            self.fields.push((
                item.ident.to_string(),
                field
                    .ident
                    .as_ref()
                    .map_or(String::new(), ToString::to_string),
                types.paths.into_iter().flatten().collect(),
            ));
        }
        visit::visit_item_struct(self, item);
    }

    fn visit_item_enum(&mut self, item: &'ast syn::ItemEnum) {
        self.definitions.insert(item.ident.to_string());
        visit::visit_item_enum(self, item);
    }
}

fn production(source: &str) -> Production {
    let mut facts = Production::default();
    facts.visit_file(&syn::parse_file(source).expect("源码应为合法 Rust"));
    facts
}

fn rust_files(directory: &Path) -> Vec<PathBuf> {
    let mut files = Vec::new();
    for entry in std::fs::read_dir(directory).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            files.extend(rust_files(&path));
        } else if path.extension().is_some_and(|extension| extension == "rs") {
            files.push(path);
        }
    }
    files.sort();
    files
}

fn has_edge(facts: &Production, module: &str) -> bool {
    facts.paths.iter().any(|path| {
        path.first().is_some_and(|head| head == "crate")
            && path.get(1).is_some_and(|target| target == module)
    })
}

fn private_context_state(facts: &Production) -> Vec<String> {
    facts
        .fields
        .iter()
        .filter_map(|(owner, field, types)| {
            (field == "history"
                || field == "sessions"
                || field == "transcript"
                || types.contains("HashMap")
                || types.contains("BTreeMap")
                || (types.contains("Vec") && types.contains("Message")))
            .then(|| format!("{owner}.{field}"))
        })
        .collect()
}

#[test]
fn entries_cannot_import_storage_runtime_or_session_concrete_types() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    for file in rust_files(&root.join("src/entry")) {
        let facts = production(&std::fs::read_to_string(&file).unwrap());
        for forbidden in [
            "storage",
            "loop_engine",
            "session",
            "context",
            "memory",
            "provider",
            "config",
            "client",
        ] {
            assert!(
                !has_edge(&facts, forbidden),
                "{} 导入禁止边 {forbidden}",
                file.display()
            );
        }
        assert!(!has_edge(&facts, "daemon"), "入口不得导入 daemon concrete");
    }
}

#[test]
fn context_has_no_private_session_map_or_message_history() {
    let source =
        std::fs::read_to_string(Path::new(env!("CARGO_MANIFEST_DIR")).join("src/context.rs"))
            .unwrap();
    let facts = production(&source);
    assert!(private_context_state(&facts).is_empty());
    for forbidden in ["daemon", "entry", "storage", "session", "memory"] {
        assert!(
            !has_edge(&facts, forbidden),
            "context 导入禁止边 {forbidden}"
        );
    }
}

#[test]
fn provider_cannot_import_tool_business_or_control_plane() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let mut files = rust_files(&root.join("src/provider"));
    files.push(root.join("src/provider.rs"));
    // wire_tests.rs 为 #[cfg(test)] 外部模块，只有测试 fixture。
    files.retain(|path| path.file_name().is_none_or(|name| name != "wire_tests.rs"));
    for file in files {
        let facts = production(&std::fs::read_to_string(&file).unwrap());
        for forbidden in [
            "tools", "daemon", "entry", "session", "memory", "safety", "storage",
        ] {
            assert!(
                !has_edge(&facts, forbidden),
                "{} 导入禁止边 {forbidden}",
                file.display()
            );
        }
    }
}

#[test]
fn long_lived_mutable_transcript_owner_cannot_multiply() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let mut owners = BTreeSet::new();
    for file in rust_files(&root.join("src")) {
        let facts = production(&std::fs::read_to_string(&file).unwrap());
        for (owner, field, types) in &facts.fields {
            if types.contains("Message")
                && types.contains("Vec")
                && (types.contains("Mutex")
                    || types.contains("RwLock")
                    || types.contains("RefCell"))
            {
                owners.insert(format!(
                    "{}::{owner}.{field}",
                    file.strip_prefix(root).unwrap().display()
                ));
            }
        }
    }
    assert!(
        owners.is_empty(),
        "长期 mutable transcript owner 不得存在：{owners:?}"
    );
}

#[test]
fn storage_cannot_depend_on_daemon_or_entry_control_owners() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    for file in rust_files(&root.join("crates/storage/src")) {
        let facts = production(&std::fs::read_to_string(&file).unwrap());
        for forbidden in [
            "daemon",
            "entry",
            "loop_engine",
            "context",
            "memory",
            "tools",
            "safety",
        ] {
            assert!(
                !has_edge(&facts, forbidden),
                "{} 导入禁止边 {forbidden}",
                file.display()
            );
        }
    }
}

#[test]
fn workspace_libraries_have_only_allowed_dependency_edges() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    for (name, allowed) in [
        ("core", vec!["serde", "serde_json", "thiserror"]),
        // JSON 仅用于有界证据渲染；不允许依赖 storage/context/daemon。
        (
            "memory",
            vec!["agent-core", "serde", "serde_json", "thiserror"],
        ),
        (
            "context",
            vec!["agent-core", "serde_json", "sha2", "thiserror"],
        ),
        (
            "daemon-client",
            vec!["agent-daemon-protocol", "serde_json", "thiserror", "tokio"],
        ),
        (
            "storage",
            vec![
                "agent-core",
                "libc",
                "rusqlite",
                "serde",
                "serde_json",
                "sha2",
                "thiserror",
                "tracing",
            ],
        ),
        (
            "daemon-protocol",
            vec!["agent-core", "serde", "serde_json", "thiserror"],
        ),
    ] {
        let source =
            std::fs::read_to_string(root.join(format!("crates/{name}/Cargo.toml"))).unwrap();
        let manifest: toml::Value = toml::from_str(&source).unwrap();
        for table in ["dependencies", "build-dependencies"] {
            if let Some(dependencies) = manifest.get(table).and_then(toml::Value::as_table) {
                for dependency in dependencies.keys() {
                    assert!(
                        allowed.contains(&dependency.as_str()),
                        "{name} 禁止依赖 {dependency}"
                    );
                }
            }
        }
        assert!(
            manifest.get("target").is_none(),
            "新增条件依赖必须纳入架构检查"
        );
    }
}

#[test]
fn compatibility_facade_does_not_redefine_protocol_or_domain_types() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    for facade in [
        "src/daemon/protocol.rs",
        "src/client.rs",
        "src/storage/mod.rs",
    ] {
        let source = std::fs::read_to_string(root.join(facade)).unwrap();
        let parsed = syn::parse_file(&source).unwrap();
        assert!(
            parsed.items.iter().all(|item| matches!(item, Item::Use(_))),
            "{facade} 不能保留平行实现"
        );
    }
    let ids = [
        "SessionKey",
        "SessionLifetimeId",
        "SessionId",
        "RunId",
        "TurnId",
        "InteractionId",
        "EventSeq",
        "ResourceId",
        "Message",
        "Role",
        "ToolCall",
        "ToolSpec",
        "RequestId",
        "PendingApprovalInfo",
        "SessionInfo",
        "SessionStatus",
        "RunStatus",
    ];
    for file in rust_files(&root.join("src")) {
        let facts = production(&std::fs::read_to_string(&file).unwrap());
        for id in ids {
            assert!(
                !facts.definitions.contains(id),
                "{} 重新定义共享类型 {id}",
                file.display()
            );
        }
    }
}

#[test]
fn checker_detects_grouped_aliased_and_fully_qualified_imports() {
    let facts = production(
        "use crate::{storage::RunStore as S, loop_engine::*}; fn f() { crate::session::load(); }",
    );
    for target in ["storage", "loop_engine", "session"] {
        assert!(has_edge(&facts, target));
    }
    let test_fixture = production(
        "#[cfg(test)] mod tests { use crate::storage::RunStore; } #[cfg(test)] use crate::loop_engine::LoopEngine;",
    );
    assert!(!has_edge(&test_fixture, "storage"));
    assert!(!has_edge(&test_fixture, "loop_engine"));
    let context = production(
        "struct Context { stash: std::sync::Mutex<Vec<Message>>, cache: std::collections::HashMap<String, usize> }",
    );
    assert_eq!(
        private_context_state(&context),
        ["Context.stash", "Context.cache"]
    );
}
