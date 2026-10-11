#!/usr/bin/env python3
"""逐项运行用户指定门禁，保留失败；不把离线、mock 或缺失环境冒充验证。"""
import argparse
import datetime
import json
import subprocess
from pathlib import Path


COMMANDS = [
    ("fmt", ["cargo", "fmt", "--all", "--", "--check"]),
    ("tests", ["cargo", "test", "--locked", "--workspace", "--all-targets", "--all-features"]),
    ("clippy", ["cargo", "clippy", "--locked", "--workspace", "--all-targets", "--all-features", "--", "-D", "warnings"]),
    ("msrv", ["cargo", "+1.88.0", "check", "--locked", "--workspace", "--all-targets", "--all-features"]),
    ("release", ["cargo", "build", "--locked", "--release"]),
    ("web", ["node", "--test", "web/app.test.cjs"]),
    ("js", ["node", "--check", "web/app.js"]),
    ("deny", ["cargo", "deny", "check"]),
    ("diff", ["git", "diff", "--check"]),
]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--wave", required=True, help="证据日志前缀，例如 wave0")
    parser.add_argument("--only", nargs="+", choices=[name for name, _ in COMMANDS])
    args = parser.parse_args()
    if not args.wave.replace("-", "").isalnum():
        parser.error("wave 只能包含字母、数字和连字符")
    root = Path(__file__).resolve().parent.parent
    evidence = root / "docs/enterprise/evidence"
    evidence.mkdir(parents=True, exist_ok=True)
    report = evidence / f"{args.wave}-gates.json"
    results = []
    for name, command in COMMANDS:
        if args.only and name not in args.only:
            continue
        log = evidence / f"{args.wave}-{name}.log"
        print(f"开始：{name}", flush=True)
        started = datetime.datetime.now(datetime.timezone.utc)
        with log.open("w") as output:
            try:
                result = subprocess.run(command, cwd=root, stdout=output, stderr=subprocess.STDOUT, check=False)
                code = result.returncode
            except OSError as error:
                output.write(f"门禁命令不可运行：{error}\n")
                code = 127
        elapsed = (datetime.datetime.now(datetime.timezone.utc) - started).total_seconds()
        results.append({"name": name, "command": command, "exit_code": code, "seconds": elapsed,
                        "started_at_utc": started.isoformat(), "log": str(log.relative_to(root)),
                        "verification": "locally verified" if code == 0 else "failed/unverified"})
        report.write_text(json.dumps(results, ensure_ascii=False, indent=2) + "\n")
        print(f"完成：{name}，退出码 {code}", flush=True)
    return 1 if any(item["exit_code"] != 0 for item in results) else 0


if __name__ == "__main__":
    raise SystemExit(main())
