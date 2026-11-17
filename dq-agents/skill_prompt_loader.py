import os
from pathlib import Path
from typing import Iterable


def resolve_skills_dir() -> Path:
    env_dir = os.getenv("DQ_SKILLS_DIR")
    if env_dir:
        return Path(env_dir).expanduser().resolve()

    # Default: <dq-agents>/skills
    return (Path(__file__).resolve().parent / "skills").resolve()


def load_skill_docs(skill_names: Iterable[str]) -> str:
    """Load skill docs as the SOURCE OF TRUTH for an agent's system prompt.

    Returns raw concatenated content for direct use as system prompt. Returns "" if no
    skill found (caller should fall back to a hardcoded prompt).
    """
    skills_dir = resolve_skills_dir()
    if not skills_dir.exists():
        print(f"WARNING: Skills directory not found: {skills_dir}. Fallback prompt will be used.")
        return ""

    sections: list[str] = []
    for skill_name in skill_names:
        skill_path = skills_dir / skill_name
        if not skill_path.exists():
            print(f"WARNING: Skill folder not found: {skill_path}. Skipping.")
            continue

        for doc_name in ("SKILL.md", "METHODOLOGY.md", "OUTPUT_FORMAT.md"):
            doc_path = skill_path / doc_name
            if not doc_path.exists():
                continue
            try:
                content = doc_path.read_text(encoding="utf-8").strip()
            except Exception as exc:
                print(f"WARNING: Failed to read {doc_path}: {exc}")
                continue
            sections.append(f"### {skill_name}/{doc_name}\n{content}")

    return "\n\n".join(sections)
