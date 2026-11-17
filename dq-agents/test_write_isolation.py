"""Chan viec mot sub-agent lay lai quyen ghi.

Thiet ke: Orchestrator la ben DUY NHAT cham vao kho luu tru. Ba sub-agent (gen / verify /
analysis) la ham thuan - nhan du lieu, tra dict. Quy dinh nay tung chi ton tai trong loi dan
cua prompt va ca ba deu vi pham trong im lang: verify_rules_agent ghi thang active.json,
analysis_agent ghi thang analysis/latest.json.

Bai test doc MA NGUON chu khong import: import keo theo langchain, ma may CI khong can no.

    python3 dq-agents/test_write_isolation.py
"""
import ast
import pathlib
import sys

#: Module chi Orchestrator duoc dung.
ORCHESTRATOR_ONLY_MODULES = {"dq_store_write", "live_channel"}

#: Ten ham ghi - chan du import theo kieu nao.
WRITE_FUNCTION_NAMES = {"write_json", "publish_rules", "save_analysis", "record_checkpoint",
                        "publish_verdict", "promote"}

#: File duoc coi la sub-agent, tuc khong duoc ghi. Dung BA: gen / verify / analysis.
#: Thuc thi rule khong phai agent - do la job Spark, tai moi checkpoint dq.check().
SUBAGENT_FILES = ("gen_rules_agent.py", "verify_rules_agent.py", "analysis_agent.py")

#: Dong BAT BUOC co trong orchestrator; thieu thi khong con ai ghi duoc.
ORCHESTRATOR_REQUIRED_LINES = (
    'os.environ.setdefault("DQ_ROLE", "orchestrator")',
    "import dq_store_write",
)


def iter_imports(tree):
    """Sinh (module_goc, ten_duoc_import) cho moi lenh import trong cay cu phap."""
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                yield alias.name.split(".")[0], alias.name
        elif isinstance(node, ast.ImportFrom):
            module = (node.module or "").split(".")[0]
            for alias in node.names:
                yield module, alias.name


def check_subagent(path):
    violations = []
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    for module, name in iter_imports(tree):
        if module in ORCHESTRATOR_ONLY_MODULES:
            violations.append(f"{path.name}: imports orchestrator-only module '{module}'")
        if name in WRITE_FUNCTION_NAMES:
            violations.append(f"{path.name}: imports write function '{name}' from '{module}'")
    return violations


def check_orchestrator(path):
    source = path.read_text(encoding="utf-8")
    return [f"{path.name}: missing required line: {line}"
            for line in ORCHESTRATOR_REQUIRED_LINES if line not in source]


def main():
    agents_dir = pathlib.Path(__file__).parent
    violations = []
    checked = 0

    for filename in SUBAGENT_FILES:
        path = agents_dir / filename
        if not path.exists():
            continue
        checked += 1
        violations += check_subagent(path)

    orchestrator = agents_dir / "orchestrator.py"
    if orchestrator.exists():
        violations += check_orchestrator(orchestrator)

    if violations:
        print("WRITE ISOLATION BROKEN:")
        for violation in violations:
            print("  -", violation)
        return 1
    print(f"OK: {checked} sub-agent khong cham duong ghi nao; the Orchestrator holds sole authority.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
