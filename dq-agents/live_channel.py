"""Kenh 2 chieu voi job Spark dang chay, qua stdin/stdout.

Khi agent duoc goi tu trong job (DqCheckpoint.java -> ProcessBuilder), job van giu SparkSession
song. Module nay bien no thanh TOOL SERVER: agent phat yeu cau ra stdout, Java thuc thi tren
session that roi tra ket qua ve stdin - thu metric aggregate cua rule khong lam duoc.

Giao thuc: JSON Lines.
    java  -> agent: {"type":"hello","checkpoint":"...","tools":[{name,description,parameters}]}
    agent -> java : {"type":"tool_request","tool":"query_live_stage","args":{"sql":"..."}}
    java  -> agent: {"type":"tool_response","ok":true,"result":{...}}
    agent -> java : {"type":"decision","action":"PROCEED","reason":"..."}

Dong "hello" den TRUOC moi thu khac: job cong bo tool cua no, module nay khong giu ban sao nao.

QUAN TRONG: stdout CHI chua dong giao thuc. Moi log phai ra stderr (xem agent_loop.py) - ghi
nham ra stdout la hong giao thuc va job se treo.
"""
import json
import sys


class LiveChannelError(RuntimeError):
    pass


_hello = None


def available() -> bool:
    """True khi dang chay duoi quyen mot job Spark (do DqCheckpoint dat bien nay)."""
    import os
    # Doc truc tiep thay vi qua dq_env: DqCheckpoint dat bien nay cho tien trinh
    # con, va module co the da duoc import truoc do trong cung tien trinh.
    return os.getenv("DQ_LIVE_CHANNEL") == "1"


def handshake() -> list:
    """Doc dong "hello" dau kenh, tra ve danh sach tool job cho phep goi.

    Goi duoc nhieu lan, chi doc stdin mot lan. Tra [] khi khong chay duoi job Spark.
    """
    global _hello
    if _hello is not None:
        return _hello.get("tools", [])
    if not available():
        _hello = {}
        return []

    line = sys.stdin.readline()
    if not line:
        raise LiveChannelError("Job Spark dong kenh truoc khi cong bo tool.")
    try:
        msg = json.loads(line)
    except json.JSONDecodeError as e:
        raise LiveChannelError(f"Dong hello khong phai JSON: {line[:200]!r}") from e
    if msg.get("type") != "hello":
        raise LiveChannelError(f"Cho hello nhung nhan: {msg.get('type')!r}")
    _hello = msg
    return msg.get("tools", [])


def _send(payload: dict) -> None:
    sys.stdout.write(json.dumps(payload, ensure_ascii=False, default=str) + "\n")
    sys.stdout.flush()


def request(tool: str, args: dict, timeout_note: str = "") -> dict:
    """Gui yeu cau sang job Java va cho tra loi. Java luon tra loi va co timeout rieng."""
    if not available():
        return {"ok": False, "error": "Khong co kenh live: agent dang khong chay trong job Spark."}

    _send({"type": "tool_request", "tool": tool, "args": args})
    line = sys.stdin.readline()
    if not line:
        raise LiveChannelError("Job Spark dong kenh giua chung (khong con doc duoc stdin).")
    try:
        resp = json.loads(line)
    except json.JSONDecodeError as e:
        raise LiveChannelError(f"Java tra ve dong khong phai JSON: {line[:200]!r}") from e
    if resp.get("type") != "tool_response":
        raise LiveChannelError(f"Cho tool_response nhung nhan: {resp.get('type')!r}")
    return resp


def send_decision(action: str, reason: str) -> None:
    """Dong giao thuc CUOI CUNG: bao cho job biet quyet dinh roi ket thuc."""
    _send({"type": "decision", "action": action, "reason": reason})
