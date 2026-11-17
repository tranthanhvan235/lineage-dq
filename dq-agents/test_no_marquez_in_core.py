"""Chan viec core lay input tu Marquez.

Marquez la lop HIEN THI cho nguoi dung cuoi, khong phai nguon du lieu. Moi thu core can deu do
chinh framework ghi ra HDFS:

    lineage   /dq/lineage/{key}/latest.json      DqGuard.publishLineage
    schema    /dq/stages/{key}/latest.json       DqGuard.publishStageManifest
    so dong   /dq/reports/{key}/history/*.json   DqReportWriter.publish

Vi sao can bai test: truoc day get_dataset_schema doc THANG Marquez va khong co nguon nao khac,
nen self-check cua Gen Rules phu thuoc vao mot dich vu hien thi. Marquez chet thi khau kiem rule
im lang khong co schema ma kiem - va da hong that mot lan (endpoint tra 500 "Ran out of memory").

Bai test doc MA NGUON chu khong import: import keo theo langchain, ma may CI khong can no.

    python3 dq-agents/test_no_marquez_in_core.py
"""
import ast
import pathlib
import sys

#: File thuoc CORE - khong duoc cham vao Marquez duoi bat ky hinh thuc nao.
CORE_FILES = ("lineage_client.py", "gen_rules_agent.py", "verify_rules_agent.py",
              "analysis_agent.py", "orchestrator.py", "pipeline_context.py",
              "dq_store.py", "dq_store_write.py", "hdfs_client.py")

#: Ten bi cam trong core. dq_env van duoc giu MARQUEZ_URL cho phan hien thi/reshape.
FORBIDDEN_NAMES = {"MARQUEZ_URL"}
FORBIDDEN_TEXT = ("marquez", "MARQUEZ")


def check(path):
    source = path.read_text(encoding="utf-8")
    violations = []

    tree = ast.parse(source, filename=str(path))
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            for alias in node.names:
                if alias.name in FORBIDDEN_NAMES:
                    violations.append(f"{path.name}: imports {alias.name} from {node.module}")
        elif isinstance(node, ast.Name) and node.id in FORBIDDEN_NAMES:
            violations.append(f"{path.name}:{node.lineno}: dung {node.id}")

    # Chuoi URL cung dem: mot f-string tro toi Marquez khong di qua import nao. Nhung chu
    # thich va docstring thi duoc phep nhac ten - chinh chung la cho GIAI THICH vi sao khong
    # dung Marquez, va cam noi ve no se khien ly do bien mat khoi ma nguon.
    doc_lines = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Expr) and isinstance(node.value, ast.Constant) \
                and isinstance(node.value.value, str):
            doc_lines.update(range(node.lineno, (node.end_lineno or node.lineno) + 1))

    for lineno, line in enumerate(source.splitlines(), 1):
        if lineno in doc_lines or line.lstrip().startswith("#"):
            continue
        if any(token in line for token in FORBIDDEN_TEXT):
            violations.append(f"{path.name}:{lineno}: nhac Marquez trong ma thuc thi: "
                              f"{line.strip()[:70]}")
    return violations


def main():
    agents_dir = pathlib.Path(__file__).parent
    violations, checked = [], 0
    for filename in CORE_FILES:
        path = agents_dir / filename
        if not path.exists():
            continue
        checked += 1
        violations += check(path)

    if violations:
        print("CORE DANG LAY INPUT TU MARQUEZ:")
        for violation in violations:
            print("  -", violation)
        return 1
    print(f"OK: {checked} file core khong cham vao Marquez.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
