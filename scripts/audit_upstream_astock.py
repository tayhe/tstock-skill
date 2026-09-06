#!/usr/bin/env python3
"""
a-stock-data 上游审计与版本比对工具
====================================
定期比对 ~/Projects/upstream/a-stock-data 的最新版本与提交日志，
提取关键 bugfix 与新增端点，为 tstock 持续升级提供灵感指引。
"""

import re
import subprocess
import sys
from pathlib import Path

# Add project root to sys.path
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import config


def get_git_info(repo_path: Path):
    if not (repo_path / ".git").exists():
        return {"error": "Not a git repository"}
    try:
        commit = subprocess.check_output(
            ["git", "rev-parse", "--short", "HEAD"],
            cwd=repo_path, text=True
        ).strip()
        last_log = subprocess.check_output(
            ["git", "log", "-n", "3", "--pretty=format:%h - %s (%cr)"],
            cwd=repo_path, text=True
        ).strip()
        branch = subprocess.check_output(
            ["git", "rev-parse", "--abbrev-ref", "HEAD"],
            cwd=repo_path, text=True
        ).strip()
        return {
            "commit": commit,
            "branch": branch,
            "logs": last_log.split("\n"),
        }
    except Exception as e:
        return {"error": str(e)}


def parse_skill_version(skill_path: Path):
    if not skill_path.exists():
        return "Unknown"
    content = skill_path.read_text(encoding="utf-8")
    # 查找类似 version: 3.8.0 或 V3.8.0
    m = re.search(r"(?:version:\s*|V)(\d+\.\d+(?:\.\d+)?)", content, re.IGNORECASE)
    if m:
        return m.group(1)
    return "Unknown"


def parse_changelog(changelog_path: Path, max_items: int = 5):
    if not changelog_path.exists():
        return []
    content = changelog_path.read_text(encoding="utf-8")
    # 提取以 ## 或 ### 开头的版本条目
    sections = re.split(r"\n(?=##\s+)", content)
    items = []
    for sec in sections[:max_items]:
        lines = [l.strip() for l in sec.split("\n") if l.strip()]
        if lines:
            title = lines[0].replace("#", "").strip()
            highlights = [l for l in lines[1:10] if l.startswith(("-", "*", "•", "🔴", "★"))]
            items.append({"title": title, "highlights": highlights})
    return items


def main():
    print("=" * 70)
    print("  a-stock-data 上游审计与版本追踪工具")
    print("=" * 70)

    repo = config.ASTOCK_DATA_ROOT
    print(f"上游路径: {repo}")
    if not repo.exists():
        print(f"[错误] 未找到上游仓库: {repo}")
        sys.exit(1)

    # 1. Git 状态
    git_info = get_git_info(repo)
    if "error" in git_info:
        print(f"Git 状态: {git_info['error']}")
    else:
        print(f"当前分支: {git_info['branch']} (Commit: {git_info['commit']})")
        print("最新提交:")
        for log in git_info["logs"]:
            print(f"  • {log}")

    # 2. 版本号
    ver = parse_skill_version(repo / "SKILL.md")
    print(f"检测到版本: v{ver}")

    # 3. 变更日志高光
    changelog = parse_changelog(repo / "CHANGELOG.md", max_items=3)
    if changelog:
        print("\n近期版本高光与重要修复:")
        for entry in changelog:
            print(f"\n  [{entry['title']}]")
            for h in entry["highlights"][:5]:
                print(f"    {h}")

    # 4. 端点覆盖度与灵感推荐
    mapping_doc = ROOT / "tstock-data-source" / "references" / "ASTOCK_MAPPING.md"
    print(f"\n端点映射规范: {mapping_doc.relative_to(ROOT) if mapping_doc.exists() else '未找到'}")
    print("\n建议关注的持续吸收候选:")
    print("  1. §8 打板与连板情绪 (ths_limit_up_pool, limit_up_sentiment) -> 注入情绪分析器")
    print("  2. §10 巨潮互动易问答 (cninfo_irm) -> 增强基本面定性催化剂")
    print("  3. §11 宏观社融与PMI (pboc_social_financing, nbs_pmi) -> 注入宏观环境评分")
    print("=" * 70)


if __name__ == "__main__":
    main()
