"""Ma nguon khong duoc lech khoi config/dq-config.yaml.

Khong the dung chung mot HANG SO giua Java va Python - hai ngon ngu, hai tien trinh. Nhung
co the dung chung mot BAN KHAI, va bat moi lech ngay tai day thay vi luc 3 gio sang.

Kieu hong nay da xay ra that 21/09/2026: DQ_QUALITY_FAIL_THRESHOLD la 0.9 trong DqConfig.java
va ConfigMap, nhung 0.8 trong dashboard/backend/config.py va .env. Dashboard cham mot lan chay
la FAIL trong khi engine cham PASS. Khong ai doc duoc vi sao, va khong mot bai kiem nao hong.

Bat BA thu:
  1. Moi khoa ma nguon doc PHAI co trong ban khai - them khoa moi ma quen khai la lo ra ngay.
  2. Mac dinh trong ma nguon PHAI khop ban khai (rieng scope=local lay local_default).
  3. Cac file SINH RA (ConfigMap, .env.example) PHAI khop ban khai - chinh sync_config --check.

    python3 dq-agents/test_config_consistent.py
"""
import re
import subprocess
import sys
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parent.parent
REGISTRY = ROOT / "config" / "dq-config.yaml"

#: (nhan, duong dan, regex bat (key, default)). Trung voi scripts/extract_config_keys.py.
SOURCES = [
    ("java", "src/main/java/org/lineagedq/spark/dq/DqConfig.java",
     re.compile(r'\b(?:str|num|dbl|bool)\("(DQ_[A-Z0-9_]+)"\s*,\s*([^)]+)\)')),
    ("python", "dq-agents/dq_env.py",
     re.compile(r'\b_(?:int|str|bool|float)\("([A-Z0-9_]+)"\s*,\s*([^)]+)\)')),
    ("dashboard", "dashboard/backend/config.py",
     re.compile(r'\b_(?:int|str|bool|float)\("([A-Z0-9_]+)"\s*,\s*([^)]+)\)')),
]

FAILURES = []


def normalize(value) -> str:
    """Dua ve MOT dang de so sanh duoc giua Java, Python va YAML.

    `5_000` va `5000`, `true` va `True`, `"python3"` va `python3` la cung mot gia tri viet
    bang ba cu phap. So sanh tho se bao lech gia - nhieu hon ca lech that.
    """
    if isinstance(value, bool):
        return "true" if value else "false"
    v = str(value).strip().rstrip(",").strip()
    if len(v) >= 2 and v[0] == v[-1] and v[0] in "\"'":
        v = v[1:-1]
    if re.fullmatch(r"-?[0-9_]+", v):
        return str(int(v.replace("_", "")))
    if re.fullmatch(r"-?[0-9]*\.[0-9]+", v):
        return str(float(v))
    return v.lower() if v.lower() in ("true", "false") else v


def main() -> int:
    if not REGISTRY.exists():
        print(f"LOI  khong thay ban khai {REGISTRY}")
        return 1
    doc = yaml.safe_load(REGISTRY.read_text(encoding="utf-8")) or {}
    declared = {k["key"]: k for k in doc.get("keys") or []}
    checked = 0

    for side, rel, pattern in SOURCES:
        path = ROOT / rel
        if not path.exists():
            FAILURES.append(f"khong thay {rel} - ban khai dang noi ve mot file da bien mat")
            continue
        for key, raw in pattern.findall(path.read_text(encoding="utf-8")):
            checked += 1
            entry = declared.get(key)
            if entry is None:
                FAILURES.append(
                    f"{rel} doc {key} nhung config/dq-config.yaml KHONG khai no."
                    " Them vao ban khai roi chay scripts/sync_config.py --write")
                continue
            # Thu tu uu tien khi doi chieu voi MA NGUON:
            #   code_default  mac dinh chot cung, co the khac mac dinh cum mot cach co y
            #   local_default cho scope=local (chay tren host)
            #   default       mac dinh cum
            if "code_default" in entry:
                want = entry["code_default"]
            elif entry.get("scope") == "local" and "local_default" in entry:
                want = entry["local_default"]
            else:
                want = entry.get("default")
            if normalize(raw) != normalize(want):
                FAILURES.append(
                    f"{key}: {rel} mac dinh {normalize(raw)!r} nhung ban khai noi"
                    f" {normalize(want)!r}")

    # Cac ban sao sinh ra co con khop khong - de sync_config tu tra loi, khong lam lai.
    proc = subprocess.run([sys.executable, str(ROOT / "scripts" / "sync_config.py"), "--check"],
                          capture_output=True, text=True)
    if proc.returncode != 0:
        FAILURES.append("file sinh ra da lech khoi ban khai:\n"
                        + (proc.stdout or proc.stderr).strip())

    if FAILURES:
        print(f"DO: {len(FAILURES)} lech giua ma nguon va config/dq-config.yaml\n")
        for f in FAILURES:
            print("  -", f)
        return 1
    print(f"OK: {checked} cho doc cau hinh trong ma nguon khop ban khai"
          f" ({len(declared)} khoa), va cac file sinh ra van dung.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
