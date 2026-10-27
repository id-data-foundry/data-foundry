#!/usr/bin/env python3
"""
DataFoundry Changelog Generator & Reconstructor
Generates and maintains CHANGELOG.md adhering to Keep a Changelog 1.1.0 and SemVer.
"""

import os
import re
import subprocess
import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
DF_DIR = SCRIPT_DIR.parent
ROOT_DIR = DF_DIR.parent

GITHUB_BASE = "https://github.com/id-data-foundry/data-foundry"
GITLAB_BASE = "https://gitlab.tue.nl/id-dept/id-projects"

SECTION_ORDER = [
    "Security",
    "Added",
    "Changed",
    "Performance",
    "Deprecated",
    "Removed",
    "Fixed",
]


def run_git(args, cwd):
    result = subprocess.run(
        ["git"] + args,
        cwd=cwd,
        capture_output=True,
        text=True,
        check=True,
    )
    return result.stdout.strip()


def parse_commit_bullets(raw_body):
    """Split commit message into individual bullet items if multiple changes are documented."""
    lines = raw_body.strip().split("\n")
    first_line = lines[0].strip() if lines else ""
    
    # Conventional commit check
    m_conv = re.match(r"^(feat|feature|fix|perf|refactor|chore|test|docs|style)(\([^\)]+\))?:\s*(.*)", first_line, re.I)
    if m_conv:
        bullets = [first_line]
        for line in lines[1:]:
            line_str = line.strip()
            if line_str.startswith(("-", "*", "•")):
                bullets.append(line_str)
        return bullets

    # Multi-item split for legacy bullets
    items = re.split(r"(?:\r?\n\s*[-–*•]|\s{2,}[-–*•])\s*", raw_body)
    cleaned = []
    for it in items:
        it = it.strip()
        if not it:
            continue
        if re.match(r"^v\d+\.\d+(\.\d+)?\b", it, re.I):
            continue
        cleaned.append(it)
    return cleaned if cleaned else [first_line]


def classify_bullet(text, commit_hash, repo_type, include_hash_link=True):
    s = text.strip()
    s = re.sub(r"^[-–*•>\s]+", "", s).strip()
    s_lower = s.lower()

    if re.match(r"^merge (branch|commit|pull request)", s_lower):
        return None
    if re.match(r"^v0\.\d+", s_lower):
        return None
    if s_lower in ["accomplishments", "documentation update", "docs bump", "code formatting, organize imports", "clean-up", "clean up"]:
        return None

    cat = "Changed"
    clean_text = s

    # Conventional commits
    m = re.match(r"^(feat|feature)(\([^\)]+\))?:\s*(.*)", s, re.I)
    if m:
        cat = "Added"
        clean_text = m.group(3)
    else:
        m = re.match(r"^fix\(security[^\)]*\):\s*(.*)", s, re.I)
        if m:
            cat = "Security"
            clean_text = m.group(1)
        else:
            m = re.match(r"^fix(\([^\)]+\))?:\s*(.*)", s, re.I)
            if m:
                cat = "Fixed"
                clean_text = m.group(2)
            else:
                m = re.match(r"^perf(\([^\)]+\))?:\s*(.*)", s, re.I)
                if m:
                    cat = "Performance"
                    clean_text = m.group(2)
                else:
                    m = re.match(r"^(refactor|chore|build|test|docs|style)(\([^\)]+\))?:\s*(.*)", s, re.I)
                    if m:
                        cat = "Changed"
                        clean_text = m.group(3)

    # Keyword detection for non-conventional commits
    if cat == "Changed":
        if any(w in s_lower for w in ["security", "cswsh", "csrf", "xss", "idor", "csp", "sandbox", "sanitize"]):
            cat = "Security"
        elif s_lower.startswith("added ") or s_lower.startswith("add ") or "added:" in s_lower:
            cat = "Added"
            clean_text = re.sub(r"^(added|add):\s*", "", clean_text, flags=re.I)
        elif s_lower.startswith("fixed ") or s_lower.startswith("fix ") or "fixed:" in s_lower:
            cat = "Fixed"
            clean_text = re.sub(r"^(fixed|fix):\s*", "", clean_text, flags=re.I)
        elif s_lower.startswith("removed ") or s_lower.startswith("remove ") or "removed:" in s_lower:
            cat = "Removed"
            clean_text = re.sub(r"^(removed|remove):\s*", "", clean_text, flags=re.I)
        elif s_lower.startswith("deprecated "):
            cat = "Deprecated"
        elif "performance" in s_lower or "speed-up" in s_lower or "optimization" in s_lower:
            cat = "Performance"

    clean_text = re.sub(r"^[-–*•>\s]+", "", clean_text).strip()
    if clean_text:
        clean_text = clean_text[0].upper() + clean_text[1:]

    if not clean_text:
        return None

    if include_hash_link and commit_hash:
        short_hash = commit_hash[:7]
        if repo_type == "github":
            link = f"{GITHUB_BASE}/commit/{commit_hash}"
        else:
            link = f"{GITLAB_BASE}/-/commit/{commit_hash}"
        return cat, f"{clean_text} ([{short_hash}]({link}))"
    else:
        return cat, clean_text


def get_commits_in_range(cwd, commit_range):
    log_output = run_git(["log", "--format=%H|%ad|%B%x00", "--date=short", commit_range], cwd=cwd)
    raw_commits = log_output.split("\x00")
    commits = []
    for rc in raw_commits:
        rc = rc.strip()
        if not rc:
            continue
        parts = rc.split("|", 2)
        if len(parts) < 3:
            continue
        h, dt, b = parts
        commits.append({"hash": h, "date": dt, "body": b.strip()})
    return commits


def format_release_section(version_str, date_str, categorized_items):
    lines = []
    if date_str == "Unreleased":
        lines.append(f"## [{version_str}] - Unreleased\n")
    else:
        lines.append(f"## [{version_str}] - {date_str}\n")

    has_content = False
    for section in SECTION_ORDER:
        items = categorized_items.get(section, [])
        if not items:
            continue
        has_content = True
        lines.append(f"### {section}\n")
        seen = set()
        for item in items:
            if item not in seen:
                seen.add(item)
                lines.append(f"- {item}\n")
        lines.append("\n")

    if not has_content:
        lines.append("- Maintenance release and dependency updates.\n\n")

    return "".join(lines)


def build_changelog():
    print("Reconstructing DataFoundry changelog...")
    
    # 1. Modern Releases in DataFoundry submodule
    df_release_specs = [
        ("v0.9.36", "Unreleased", "v0.9.35..HEAD", "github"),
        ("v0.9.35", "2026-10-12", "v0.9.34..v0.9.35", "github"),
        ("v0.9.34", "2026-10-12", "v0.9.33..v0.9.34", "github"),
        ("v0.9.33", "2026-10-01", "v0.9.32..v0.9.33", "github"),
        ("v0.9.32", "2026-09-21", "v0.9.31..v0.9.32", "github"),
        ("v0.9.31", "2026-09-21", "v0.9.30..v0.9.31", "github"),
        ("v0.9.30", "2026-09-07", "v0.9.29..v0.9.30", "github"),
        ("v0.9.29", "2026-08-28", "v0.9.28..v0.9.29", "github"),
        ("v0.9.28", "2026-08-28", "v0.9.27..v0.9.28", "github"),
        ("v0.9.27", "2026-08-28", "v0.9.26..v0.9.27", "github"),
        ("v0.9.26", "2026-06-28", "v0.9.25..v0.9.26", "github"),
        ("v0.9.25", "2026-06-11", "v0.9.24..v0.9.25", "github"),
        ("v0.9.24", "2026-05-30", "ff7c031..v0.9.24", "github"),
        ("v0.9.23", "2026-05-27", "v0.9.22..ff7c031", "github"),
        ("v0.9.22", "2026-05-16", "v0.9.21..v0.9.22", "github"),
        ("v0.9.21", "2026-05-01", "v0.9.20..v0.9.21", "github"),
        ("v0.9.20", "2026-04-27", "v0.9.19..v0.9.20", "github"),
        ("v0.9.19", "2026-04-24", "v0.9.18..v0.9.19", "github"),
        ("v0.9.18", "2026-04-18", "v0.9.17..v0.9.18", "github"),
        ("v0.9.17", "2026-03-22", "v0.9.16..v0.9.17", "github"),
        ("v0.9.16", "2026-03-15", "v0.9.15..v0.9.16", "github"),
        ("v0.9.15", "2026-02-16", "300ef56..v0.9.15", "github"),
    ]

    # 2. Modern Releases in IDATI root repo prior to submodule split
    root_release_specs = [
        ("v0.9.14", "2026-02-08", "cba97f7b..c115e9a8", "gitlab"),
        ("v0.9.13", "2026-01-30", "8ba91ec0..cba97f7b", "gitlab"),
        ("v0.9.12", "2026-01-26", "faed795f..8ba91ec0", "gitlab"),
        ("v0.9.10", "2025-12-18", "427c70f9..faed795f", "gitlab"),
        ("v0.9.9",  "2025-12-11", "c1abe6be..427c70f9", "gitlab"),
        ("v0.9.8",  "2025-11-11", "c946fa8d..c1abe6be", "gitlab"),
        ("v0.9.7",  "2025-11-07", "46f1546a..c946fa8d", "gitlab"),
        ("v0.9.6",  "2025-10-29", "0e6699c8..46f1546a", "gitlab"),
        ("v0.9.5",  "2025-10-20", "017801b3..0e6699c8", "gitlab"),
        ("v0.9.4",  "2025-10-03", "6d427552..017801b3", "gitlab"),
        ("v0.9.3",  "2025-09-24", "d9561cac..6d427552", "gitlab"),
        ("v0.9.2",  "2025-09-24", "ef2d2f67..d9561cac", "gitlab"),
        ("v0.9.1",  "2025-08-31", "45f1cc4c..ef2d2f67", "gitlab"),
        ("v0.9.0",  "2025-08-03", "ac21ceb0..45f1cc4c", "gitlab"),
    ]

    out = []
    out.append("# Changelog\n\n")
    out.append("All notable changes to this project will be documented in this file.\n\n")
    out.append("The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),\n")
    out.append("and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).\n\n")

    # Process DataFoundry submodule releases (v0.9.36 down to v0.9.15)
    for ver, dt, rng, repo_type in df_release_specs:
        commits = get_commits_in_range(DF_DIR, rng)
        categorized = {}
        for c in commits:
            bullets = parse_commit_bullets(c["body"])
            for b in bullets:
                res = classify_bullet(b, c["hash"], repo_type, include_hash_link=True)
                if res:
                    cat, item_text = res
                    categorized.setdefault(cat, []).append(item_text)
        out.append(format_release_section(ver, dt, categorized))

    # Process IDATI root releases (v0.9.14 down to v0.9.0)
    for ver, dt, rng, repo_type in root_release_specs:
        commits = get_commits_in_range(ROOT_DIR, rng)
        categorized = {}
        for c in commits:
            bullets = parse_commit_bullets(c["body"])
            for b in bullets:
                res = classify_bullet(b, c["hash"], repo_type, include_hash_link=True)
                if res:
                    cat, item_text = res
                    categorized.setdefault(cat, []).append(item_text)
        out.append(format_release_section(ver, dt, categorized))

    # Historical Series Summaries (v0.8.x down to v0.4.x)
    out.append("## Historical Releases (Pre-v0.9.0)\n\n")

    out.append("### [v0.8.x] - 2023-05-15 to 2025-07-28 (60 Releases: v0.8.0 – v0.8.59)\n\n")
    out.append("- **Local AI & LLMs**: Integrated local generative AI services, OpenAI-compatible proxy, ConvoScript runtime, and ViperIDE integration.\n")
    out.append("- **Interactive Notebooks**: Added Starboard notebook support with Pyodide client-side Python execution.\n")
    out.append("- **Framework Upgrade**: Upgraded to Play Framework 3.0.x and Apache Pekko runtime.\n")
    out.append("- **Survey & Media Tools**: Introduced narrative surveys, speech-to-text audio processing, and Telegram bot data collection.\n")
    out.append("- **Platform Views**: Created student showcase views, portfolio generation engines, and demoday visitor interaction flows.\n\n")

    out.append("### [v0.7.x] - 2021-10-18 to 2023-05-09 (54 Releases: v0.7.0 – v0.7.53)\n\n")
    out.append("- **Scripting Engine**: Integrated GraalVM polyglot JavaScript runtime for dynamic dataset transformations and custom reactive actors.\n")
    out.append("- **HTMX Adoption**: Initiated progressive migration to HTMX for interactive UI flows without full page reloads.\n")
    out.append("- **Materialize CSS Modernisation**: Upgraded to Materialize CSS 1.1 and modernized core component libraries.\n")
    out.append("- **Database & Evolutions**: Enhanced Ebean ORM migrations, query optimizations, and automated schema evolution scripts.\n")
    out.append("- **Analytics & Projections**: Advanced timeseries downsampling, CSV projections, and OOCSI stream watchdog monitoring.\n\n")

    out.append("### [v0.6.x] - 2021-03-01 to 2021-10-18 (31 Releases: v0.6.0 – v0.6.30)\n\n")
    out.append("- **Production Graduation**: Official graduation out of beta with the `v0.6.0` release.\n")
    out.append("- **OpenAPI / Swagger**: Designed and deployed OpenAPI v2 documentation and standardized REST endpoints.\n")
    out.append("- **File-based Buffering**: Implemented disk-backed buffering for high-throughput dataset exports (preventing memory exhaustion on 10k+ record sets).\n")
    out.append("- **Study Management**: Multi-dataset participant sign-up, authorization token hashing, and GDPR-aligned data access rules.\n\n")

    out.append("### [v0.5.x] - 2020-01-26 to 2021-03-01 (48 Releases: v0.5.0 – v0.5.50)\n\n")
    out.append("- **Multi-Dataset Architecture**: Introduced specialized dataset categories: IoT (timeseries), Entity (key-value), Media, Diary, Survey, and Wearables (Fitbit & Google Fit).\n")
    out.append("- **OOCSI Real-time Ecosystem**: Integrated bi-directional OOCSI client connectivity for real-time messaging and device telemetry.\n")
    out.append("- **Device & Cluster Management**: Added physical cluster partitioning, device provisioning, and participant mapping.\n")
    out.append("- **Security & Administration**: Added automated database backup tools, server watchdog monitoring, and domain verification hosting.\n\n")

    out.append("### [v0.4.x] - 2019-11-03 to 2020-01-23 (4 Releases: v0.4.13 – v0.4.30)\n\n")
    out.append("- **Core Foundation**: Established initial Play Framework backend architecture, Ebean data models, and routing hierarchy.\n")
    out.append("- **Access Control**: Initial implementation of user authentication, project ownership, collaborator roles, and guest view permissions.\n")
    out.append("- **Data Ingestion**: Basic streaming CSV data export and timeseries record form ingestion stubs.\n\n")

    # Links Section at the bottom
    out.append("---\n\n")
    out.append(f"[v0.9.36]: {GITHUB_BASE}/compare/v0.9.35...development\n")
    out.append(f"[v0.9.35]: {GITHUB_BASE}/compare/v0.9.34...v0.9.35\n")
    out.append(f"[v0.9.34]: {GITHUB_BASE}/compare/v0.9.33...v0.9.34\n")
    out.append(f"[v0.9.33]: {GITHUB_BASE}/compare/v0.9.32...v0.9.33\n")
    out.append(f"[v0.9.32]: {GITHUB_BASE}/compare/v0.9.31...v0.9.32\n")
    out.append(f"[v0.9.31]: {GITHUB_BASE}/compare/v0.9.30...v0.9.31\n")
    out.append(f"[v0.9.30]: {GITHUB_BASE}/compare/v0.9.29...v0.9.30\n")
    out.append(f"[v0.9.29]: {GITHUB_BASE}/compare/v0.9.28...v0.9.29\n")
    out.append(f"[v0.9.28]: {GITHUB_BASE}/compare/v0.9.27...v0.9.28\n")
    out.append(f"[v0.9.27]: {GITHUB_BASE}/compare/v0.9.26...v0.9.27\n")
    out.append(f"[v0.9.26]: {GITHUB_BASE}/compare/v0.9.25...v0.9.26\n")
    out.append(f"[v0.9.25]: {GITHUB_BASE}/compare/v0.9.24...v0.9.25\n")
    out.append(f"[v0.9.24]: {GITHUB_BASE}/compare/v0.9.22...v0.9.24\n")
    out.append(f"[v0.9.23]: {GITHUB_BASE}/commit/ff7c031\n")
    out.append(f"[v0.9.22]: {GITHUB_BASE}/compare/v0.9.21...v0.9.22\n")
    out.append(f"[v0.9.21]: {GITHUB_BASE}/compare/v0.9.20...v0.9.21\n")
    out.append(f"[v0.9.20]: {GITHUB_BASE}/compare/v0.9.19...v0.9.20\n")
    out.append(f"[v0.9.19]: {GITHUB_BASE}/compare/v0.9.18...v0.9.19\n")
    out.append(f"[v0.9.18]: {GITHUB_BASE}/compare/v0.9.17...v0.9.18\n")
    out.append(f"[v0.9.17]: {GITHUB_BASE}/compare/v0.9.16...v0.9.17\n")
    out.append(f"[v0.9.16]: {GITHUB_BASE}/compare/v0.9.15...v0.9.16\n")
    out.append(f"[v0.9.15]: {GITHUB_BASE}/releases/tag/v0.9.15\n")
    out.append(f"[v0.9.14]: {GITLAB_BASE}/-/compare/cba97f7b...c115e9a8\n")
    out.append(f"[v0.9.13]: {GITLAB_BASE}/-/compare/8ba91ec0...cba97f7b\n")
    out.append(f"[v0.9.12]: {GITLAB_BASE}/-/compare/faed795f...8ba91ec0\n")
    out.append(f"[v0.9.10]: {GITLAB_BASE}/-/compare/427c70f9...faed795f\n")
    out.append(f"[v0.9.9]:  {GITLAB_BASE}/-/compare/c1abe6be...427c70f9\n")
    out.append(f"[v0.9.8]:  {GITLAB_BASE}/-/compare/c946fa8d...c1abe6be\n")
    out.append(f"[v0.9.7]:  {GITLAB_BASE}/-/compare/46f1546a...c946fa8d\n")
    out.append(f"[v0.9.6]:  {GITLAB_BASE}/-/compare/0e6699c8...46f1546a\n")
    out.append(f"[v0.9.5]:  {GITLAB_BASE}/-/compare/017801b3...0e6699c8\n")
    out.append(f"[v0.9.4]:  {GITLAB_BASE}/-/compare/6d427552...017801b3\n")
    out.append(f"[v0.9.3]:  {GITLAB_BASE}/-/compare/d9561cac...6d427552\n")
    out.append(f"[v0.9.2]:  {GITLAB_BASE}/-/compare/ef2d2f67...d9561cac\n")
    out.append(f"[v0.9.1]:  {GITLAB_BASE}/-/compare/45f1cc4c...ef2d2f67\n")
    out.append(f"[v0.9.0]:  {GITLAB_BASE}/-/compare/ac21ceb0...45f1cc4c\n")

    content = "".join(out)
    target_path = DF_DIR / "CHANGELOG.md"
    target_path.write_text(content, encoding="utf-8")
    print(f"Successfully generated {target_path} ({len(content)} bytes)")


if __name__ == "__main__":
    build_changelog()
