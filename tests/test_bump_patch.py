"""`scripts/bump_patch.py`：release.yml 自动升补丁版本用的那段逻辑。

发版脚本坏了的代价是「发出去一个错的版本号 / 空的发布说明」，而且 PyPI 上的版本号
发了就收不回来——所以在本地就用一个临时 git 仓库把它整条跑一遍。
"""

from __future__ import annotations

import importlib.util
import subprocess
from pathlib import Path
from types import ModuleType

import pytest

_SCRIPT = Path(__file__).resolve().parent.parent / "scripts" / "bump_patch.py"


@pytest.fixture(scope="module")
def bp() -> ModuleType:
    spec = importlib.util.spec_from_file_location("bump_patch", _SCRIPT)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _git(root: Path, *args: str) -> None:
    subprocess.run(["git", *args], cwd=root, check=True, capture_output=True)


@pytest.fixture
def repo(tmp_path: Path) -> Path:
    (tmp_path / "netspy").mkdir()
    (tmp_path / "netspy" / "__about__.py").write_text(
        '"""版本号说明，里面也有个 "1.0.0" 字样。"""\n\n__version__ = "1.0.0"\n',
        encoding="utf-8",
    )
    (tmp_path / "CHANGELOG.md").write_text(
        "# Changelog\n\n说明。\n\n## [1.0.0] - 2026-01-01\n\n首个版本。\n", encoding="utf-8"
    )
    _git(tmp_path, "init", "-q", "-b", "main")
    _git(
        tmp_path,
        "-c",
        "user.name=t",
        "-c",
        "user.email=t@t",
        "commit",
        "-qam",
        "init",
        "--allow-empty",
    )
    _git(tmp_path, "add", ".")
    _git(tmp_path, "-c", "user.name=t", "-c", "user.email=t@t", "commit", "-qm", "init files")
    _git(tmp_path, "tag", "v1.0.0")
    return tmp_path


def _commit(root: Path, subject: str) -> None:
    _git(
        root,
        "-c",
        "user.name=t",
        "-c",
        "user.email=t@t",
        "commit",
        "-q",
        "--allow-empty",
        "-m",
        subject,
    )


# ---------------------------------------------------------------- 版本号
@pytest.mark.parametrize(
    ("old", "new"), [("1.3.2", "1.3.3"), ("1.9.9", "1.9.10"), ("2.0.0", "2.0.1")]
)
def test_next_patch(bp: ModuleType, old: str, new: str) -> None:
    assert bp.next_patch(old) == new


@pytest.mark.parametrize("version", ["1.4.0rc1", "1.4.0b2", "1.4", "v1.4.0"])
def test_refuses_to_guess_for_non_plain_versions(bp: ModuleType, version: str) -> None:
    with pytest.raises(bp.BumpError):
        bp.next_patch(version)


# ---------------------------------------------------------------- CHANGELOG
_BASE = "# Changelog\n\n## [1.0.0] - 2026-01-01\n\nold\n"


def test_handwritten_section_is_kept(bp: ModuleType) -> None:
    text = "# Changelog\n\n## [1.0.1] - 2026-02-02\n\n手写的说明\n\n" + _BASE[13:]
    assert bp.update_changelog(text, "1.0.1", "2026-03-03", ["fix: x"]) == text


def test_unreleased_section_becomes_the_release(bp: ModuleType) -> None:
    text = "# Changelog\n\n## [Unreleased]\n\n### 修复\n\n- 精心写的\n\n" + _BASE[13:]
    out = bp.update_changelog(text, "1.0.1", "2026-03-03", ["fix: 别用这个"])
    assert "## [1.0.1] - 2026-03-03\n\n### 修复\n\n- 精心写的" in out
    assert "Unreleased" not in out and "别用这个" not in out


def test_generated_section_goes_above_the_latest_release(bp: ModuleType) -> None:
    out = bp.update_changelog(_BASE, "1.0.1", "2026-03-03", ["fix: a", "feat: b"])
    assert out.index("## [1.0.1]") < out.index("## [1.0.0]")
    assert "- fix: a\n- feat: b" in out


def test_nothing_to_release_is_an_error(bp: ModuleType) -> None:
    with pytest.raises(bp.BumpError, match="没有可发布的改动"):
        bp.update_changelog(_BASE, "1.0.1", "2026-03-03", [])


# ---------------------------------------------------------------- 整条跑一遍
def test_bump_end_to_end(bp: ModuleType, repo: Path) -> None:
    _commit(repo, "fix(x): 修了一个 bug")
    _commit(repo, "release: 不该出现在说明里")

    assert bp.bump(repo, date="2026-03-03") == "1.0.1"

    about = (repo / "netspy" / "__about__.py").read_text(encoding="utf-8")
    assert '__version__ = "1.0.1"' in about
    assert '"1.0.0" 字样' in about, "文档字符串里的版本字样被误改了"
    changelog = (repo / "CHANGELOG.md").read_text(encoding="utf-8")
    assert "## [1.0.1] - 2026-03-03" in changelog
    assert "- fix(x): 修了一个 bug" in changelog
    assert "不该出现在说明里" not in changelog


def test_dry_run_changes_nothing(bp: ModuleType, repo: Path) -> None:
    _commit(repo, "fix: a")
    before = {p: p.read_bytes() for p in (repo / "netspy" / "__about__.py", repo / "CHANGELOG.md")}
    assert bp.bump(repo, date="2026-03-03", dry_run=True) == "1.0.1"
    assert all(p.read_bytes() == data for p, data in before.items())


def test_no_new_commits_fails_without_touching_files(bp: ModuleType, repo: Path) -> None:
    about = repo / "netspy" / "__about__.py"
    before = about.read_bytes()
    assert bp.main(["--root", str(repo)]) == 1
    assert about.read_bytes() == before
