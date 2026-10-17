"""把版本号最后一位 +1，并准备好 CHANGELOG 里这一版的段落。

`release.yml` 手动触发「自动升级补丁版本」时调用；本地也能直接跑来预览::

    python scripts/bump_patch.py            # 改写文件，stdout 打印新版本号
    python scripts/bump_patch.py --dry-run  # 只打印，不改文件

CHANGELOG 按这个优先级处理（`release.yml` 的 GitHub Release 正文就是从这一段抽的）：

1. 已经有 `## [新版本]` 段落 → 原样保留（你提前写好了发布说明）；
2. 有 `## [Unreleased]` 段落 → 改名成 `## [新版本] - 日期`；
3. 都没有 → 用上一个 tag 以来的提交标题生成一段（跳过 `release:` 和 merge 提交）。

上一个 tag 以来一个提交都没有、也没有手写的段落时直接失败——没有改动就不该发版。
"""

from __future__ import annotations

import argparse
import datetime as dt
import re
import subprocess
import sys
from pathlib import Path

# 必须锚在 __version__ 上：__about__.py 的文档字符串在前面，宽松的匹配会先撞上它
_VERSION_RE = re.compile(r'^__version__\s*=\s*"([^"]+)"', re.M)
_PLAIN = re.compile(r"^(\d+)\.(\d+)\.(\d+)$")


class BumpError(Exception):
    pass


def next_patch(version: str) -> str:
    m = _PLAIN.match(version)
    if not m:
        # 1.4.0rc1 的「下一个补丁版本」是什么没有唯一答案，别替人猜
        raise BumpError(f"只能给正式版本升补丁号，当前是 {version!r}（预发布版请手动改）")
    major, minor, patch = (int(x) for x in m.groups())
    return f"{major}.{minor}.{patch + 1}"


def _commits_since(root: Path, tag: str) -> list[str]:
    try:
        out = subprocess.run(
            ["git", "log", "--no-merges", "--format=%s", f"{tag}..HEAD"],
            cwd=root,
            capture_output=True,
            text=True,
            check=True,
        ).stdout
    except subprocess.CalledProcessError as exc:
        raise BumpError(f"读不到 {tag} 以来的提交（tag 不存在？）：{exc.stderr.strip()}") from exc
    return [s for s in out.splitlines() if s.strip() and not s.startswith("release:")]


def update_changelog(text: str, version: str, date: str, commits: list[str]) -> str:
    if re.search(rf"^## \[{re.escape(version)}\]", text, re.M):
        return text
    unreleased = re.search(r"^## \[Unreleased\][^\n]*$", text, re.M | re.I)
    if unreleased:
        return text[: unreleased.start()] + f"## [{version}] - {date}" + text[unreleased.end() :]
    if not commits:
        raise BumpError(
            "上一个 tag 以来没有任何提交，CHANGELOG 里也没有待发布的段落——没有可发布的改动"
        )
    body = "\n".join(f"- {s}" for s in commits)
    section = f"## [{version}] - {date}\n\n### 变更\n\n{body}\n\n"
    first = re.search(r"^## \[", text, re.M)
    if first is None:
        return text.rstrip("\n") + "\n\n" + section
    return text[: first.start()] + section + text[first.start() :]


def bump(root: Path, *, date: str, dry_run: bool = False) -> str:
    about = root / "netspy" / "__about__.py"
    changelog = root / "CHANGELOG.md"
    about_text = about.read_text(encoding="utf-8")
    m = _VERSION_RE.search(about_text)
    if not m:
        raise BumpError(f"{about} 里找不到 __version__")
    old = m.group(1)
    new = next_patch(old)
    new_changelog = update_changelog(
        changelog.read_text(encoding="utf-8"), new, date, _commits_since(root, f"v{old}")
    )
    if not dry_run:
        about.write_text(about_text[: m.start(1)] + new + about_text[m.end(1) :], encoding="utf-8")
        changelog.write_text(new_changelog, encoding="utf-8")
    return new


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parent.parent)
    parser.add_argument("--date", default=dt.date.today().isoformat())
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)
    try:
        print(bump(args.root, date=args.date, dry_run=args.dry_run))
    except BumpError as exc:
        print(f"bump_patch: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
