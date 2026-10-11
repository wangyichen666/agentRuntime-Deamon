#!/usr/bin/env python3
"""在隔离副本注入实际违规，要求架构测试失败；不修改原工作区。"""
import json
import os
import shutil
import subprocess
import tempfile
from pathlib import Path


CASES = [
    ("dependency", "crates/gateway/Cargo.toml",
     "\n[target.'cfg(unix)'.dependencies]\nagent-daemon = { version = '=0.1.0', path = '../daemon' }\n",
     "enterprise_every_workspace_library_is_reviewed_and_checks_all_cargo_dependency_tables",
     "gateway 有禁止依赖"),
    ("owner", "crates/gateway/src/serve.rs",
     "\n#[allow(dead_code)]\nstruct EnterpriseRedShadowOwner { terminals: std::sync::Mutex<Vec<agent_core::RunRecord>> }\n",
     "enterprise_workspace_has_no_second_mutable_business_owner", "有影子业务 owner"),
    ("channel", "crates/gateway/src/serve.rs",
     "\n#[allow(dead_code)]\nfn enterprise_red_unbounded() { let _ = tokio::sync::mpsc::unbounded_channel::<u8>(); }\n",
     "enterprise_channels_only_allow_exact_documented_legacy_sites_and_no_new_unbounded",
     "新无界通道、例外重复或已删除未收口都必须失败"),
]


def main():
    root = Path(__file__).resolve().parent.parent
    evidence = root / "docs/enterprise/evidence"
    evidence.mkdir(parents=True, exist_ok=True)
    results = []
    with tempfile.TemporaryDirectory(prefix="agent-enterprise-red-") as directory:
        replica = Path(directory) / "repo"
        shutil.copytree(root, replica,
                        ignore=shutil.ignore_patterns(".git", "target", ".my-agent", ".env", ".env.*", "evidence", "__pycache__"))
        # 复用编译缓存，不复用运行中的业务数据库或密钥。
        environment = dict(os.environ, CARGO_TARGET_DIR=str(root / "target"))
        for name, relative, injection, test, expected in CASES:
            path = replica / relative
            original = path.read_bytes()
            path.write_bytes(original + injection.encode())
            command = ["cargo", "test", "--locked", "--test", "architecture_contract", "--all-features", test]
            print(f"验证 RED：{name}", flush=True)
            try:
                result = subprocess.run(command, cwd=replica, env=environment, text=True,
                                        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, check=False)
            finally:
                path.write_bytes(original)
            log = evidence / f"wave0-red-{name}.log"
            log.write_text(result.stdout)
            rejected = result.returncode == 101 and expected in result.stdout and "test result: FAILED" in result.stdout
            results.append({"case": name, "command": command, "exit_code": result.returncode,
                            "expected_guard_rejected": rejected, "log": str(log.relative_to(root))})
            print(f"RED：{name}，守卫拒绝={rejected}，退出码={result.returncode}", flush=True)
    (evidence / "wave0-red-fixtures.json").write_text(json.dumps(results, ensure_ascii=False, indent=2) + "\n")
    return 0 if all(item["expected_guard_rejected"] for item in results) else 1


if __name__ == "__main__":
    raise SystemExit(main())
