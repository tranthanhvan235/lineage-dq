"""Ba ban do hanh dong checkpoint phai trung nhau, va khong cho ep nhi phan tro lai.

Tap hanh dong hien song o BA noi doc lap:

    dq-agents/orchestrator.py   _CHECKPOINT_ACTIONS   (agent duoc phep tra ve gi)
    agent-api/protocol.py       CHECKPOINT_ACTIONS    (gi di qua duoc day HTTP)
    dq/DqCheckpoint.java        enum Action           (Java hieu duoc gi)

Lech mot cai la hanh dong moi bi AM THAM ha xuong PROCEED - khong exception, khong log, job
chay tiep nhu khong co gi. Da xay ra that voi AWAIT_HUMAN: agent tra dung, ba ham trong
agent-api deu viet `"HALT" if action == "HALT" else "PROCEED"` nen Java khong bao gio nam cho,
va luot thu end-to-end ghi thang xuong bang dich.

    python3 test_checkpoint_actions.py
"""
import ast
import os
import re
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
ORCHESTRATOR = os.path.join(HERE, "orchestrator.py")
PROTOCOL = os.path.join(ROOT, "agent-api", "protocol.py")
JAVA_ENUM = os.path.join(ROOT, "src", "main", "java", "org", "lineagedq",
                         "spark", "dq", "DqCheckpoint.java")
AGENT_API_DIR = os.path.join(ROOT, "agent-api")

#: `"HALT" if x == "HALT" else "PROCEED"` va moi bien the - dung phep ep da gay ra su co.
BINARY_COERCION = re.compile(r'==\s*["\']HALT["\']\s*else|["\']HALT["\']\s+if\b')


def tuple_from_assign(path, name):
    """Doc hang so tu ma nguon bang AST - khong import, vi ca hai module deu co tac dung phu.

    Phan tu co the la TEN mot hang so khac chu khong chi chuoi tran, vi du
    `_CHECKPOINT_ACTIONS = (..., _AWAIT_HUMAN)`. Do la cach viet dung - dat ten cho mot gia tri
    duoc dung o nhieu cho - nen luoi phai theo duoc, chu khong bat nguoi ta viet xau di.
    """
    tree = ast.parse(open(path, encoding="utf-8").read(), filename=path)
    strings = {}
    for node in ast.walk(tree):
        if not isinstance(node, ast.Assign):
            continue
        if (isinstance(node.value, ast.Constant) and isinstance(node.value.value, str)
                and len(node.targets) == 1 and isinstance(node.targets[0], ast.Name)):
            strings[node.targets[0].id] = node.value.value
    for node in ast.walk(tree):
        if not isinstance(node, ast.Assign):
            continue
        for target in node.targets:
            if not (isinstance(target, ast.Name) and target.id == name):
                continue
            out = []
            for element in node.value.elts:
                if isinstance(element, ast.Constant):
                    out.append(element.value)
                elif isinstance(element, ast.Name) and element.id in strings:
                    out.append(strings[element.id])
                else:
                    sys.exit(f"{path}: phan tu cua {name} khong phai chuoi hay ten hang so")
            return tuple(out)
    sys.exit(f"Khong tim thay hang so {name} trong {path}")


def java_enum_constants(path):
    """Ten hang trong `public enum Action { ... }`, bo qua javadoc va than phuong thuc."""
    source = open(path, encoding="utf-8").read()
    start = source.index("enum Action")
    body = source[source.index("{", start) + 1:]
    body = re.sub(r"/\*.*?\*/", " ", body, flags=re.DOTALL)
    body = re.sub(r"//[^\n]*", " ", body)
    head = body.split(";")[0]
    return tuple(t for t in re.findall(r"\b[A-Z][A-Z_]+\b", head))


def scan_binary_coercion(directory):
    hits = []
    for name in sorted(os.listdir(directory)):
        if not name.endswith(".py"):
            continue
        path = os.path.join(directory, name)
        tree = ast.parse(open(path, encoding="utf-8").read(), filename=path)
        # Docstring giai thich loi cu duoc phep nhac lai phep ep - chi cam ma THUC THI.
        # Chi lay DOCSTRING chu khong phai moi ast.Constant kieu str: `"HALT"` trong chinh
        # dong ma cung la mot Constant, gom ca vao thi luoi tu bit mat cua no.
        skip = set()
        for node in ast.walk(tree):
            if not isinstance(node, (ast.Module, ast.FunctionDef,
                                     ast.AsyncFunctionDef, ast.ClassDef)):
                continue
            body = getattr(node, "body", None)
            if not body or not isinstance(body[0], ast.Expr):
                continue
            first = body[0].value
            if isinstance(first, ast.Constant) and isinstance(first.value, str):
                skip.update(range(first.lineno, (first.end_lineno or first.lineno) + 1))
        for number, line in enumerate(open(path, encoding="utf-8"), 1):
            if number in skip or line.lstrip().startswith("#"):
                continue
            if BINARY_COERCION.search(line):
                hits.append(f"{name}:{number}: {line.strip()}")
    return hits


def main():
    orchestrator = tuple_from_assign(ORCHESTRATOR, "_CHECKPOINT_ACTIONS")
    protocol = tuple_from_assign(PROTOCOL, "CHECKPOINT_ACTIONS")
    java = java_enum_constants(JAVA_ENUM)

    failed = False
    print(f"orchestrator.py : {', '.join(orchestrator)}")
    print(f"protocol.py     : {', '.join(protocol)}")
    print(f"DqCheckpoint    : {', '.join(java)}")

    # So theo TAP: thu tu khac nhau khong sao, thieu/thua mot ten moi la loi.
    for label, other in (("protocol.py", protocol), ("DqCheckpoint.java", java)):
        missing = set(orchestrator) - set(other)
        extra = set(other) - set(orchestrator)
        if missing or extra:
            failed = True
            print(f"LECH {label}:"
                  + (f" thieu {sorted(missing)}" if missing else "")
                  + (f" thua {sorted(extra)}" if extra else ""))

    hits = scan_binary_coercion(AGENT_API_DIR)
    if hits:
        failed = True
        print("\nCon phep ep nhi phan HALT/PROCEED trong agent-api - dung protocol.parse_action:")
        for hit in hits:
            print(f"  {hit}")

    print()
    if failed:
        print("HONG: ban do hanh dong khong khop - hanh dong moi se bi ha xuong PROCEED.")
        return 1
    print(f"OK: {len(orchestrator)} hanh dong khop o ca ba noi, khong con phep ep nhi phan.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
