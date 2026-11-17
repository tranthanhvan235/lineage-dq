"""Centralized /dq/ HDFS layout + path-keyed read/write helpers shared by all DQ subagents.

Single source of truth for the HDFS path scheme, so it can change (or move to a future
standalone DQ service) without hunting down duplicated f-strings across gen_rules_agent.py,
and analysis_agent.py.

Rules, analysis and checkpoints are written here; reports and lineage are written Java-side and
only read back. This JSON is the source of truth -- the Iceberg tables mirror it for querying
and nothing reads them back.
"""
import json
import os

from hdfs_client import read_json

from dq_env import DQ_ROOT  # duong dan goc, doi theo cum
from dq_env import DQ_TRANSCRIPT_MAX_CHARS


def rules_active_path(key: str) -> str:
    return f"{DQ_ROOT}/rules/{key}/active.json"


def rules_history_path(key: str, run_id: str) -> str:
    return f"{DQ_ROOT}/rules/{key}/history/{run_id}.json"


def reports_latest_path(key: str) -> str:
    return f"{DQ_ROOT}/reports/{key}/latest.json"


def analysis_latest_path(key: str) -> str:
    return f"{DQ_ROOT}/analysis/{key}/latest.json"


def analysis_history_path(key: str, analysis_id: str) -> str:
    return f"{DQ_ROOT}/analysis/{key}/history/{analysis_id}.json"


def get_active_rules(key: str):
    return read_json(rules_active_path(key))


def verification_path(key: str, generation_run_id: str) -> str:
    """Verdict cua Verify Rules agent cho mot generation_run_id.

    Java (DqRuleLoader.verificationPath) doc dung duong nay va TU CHOI nap rule set neu khong
    co verdict khop - do la cho khien viec tu choi co hieu luc that.
    """
    return f"{DQ_ROOT}/rules/{key}/verification/{generation_run_id}.json"


def get_verification(key: str, generation_run_id: str):
    return read_json(verification_path(key, generation_run_id))


def get_latest_report(key: str):
    return read_json(reports_latest_path(key))


def get_latest_analysis(key: str):
    return read_json(analysis_latest_path(key))


def stages_latest_path(key: str) -> str:
    """Manifest checkpoint do job Java ghi: danh sach stage + schema tung diem + so dong.

    Job chi biet schema cua mot stage KHI da chay qua stage do, nen agent doc manifest cua LAN
    CHAY TRUOC de sinh rule cho lan sau - cung mo hinh voi /dq/lineage/{key}/latest.json.
    """
    return f"{DQ_ROOT}/stages/{key}/latest.json"


def get_stages(key: str):
    return read_json(stages_latest_path(key))


def lineage_latest_path(key: str) -> str:
    return f"{DQ_ROOT}/lineage/{key}/latest.json"


def get_lineage(key: str):
    return read_json(lineage_latest_path(key))


def decision_path(key: str, run_id: str, stage: str) -> str:
    """Ban ghi quyet dinh cua NGUOI tai mot checkpoint trong mot lan chay.

    Java doc dung duong nay trong vong cho (DqHumanDecision.path), va CLI dq-decide.sh ghi
    cau tra loi vao dung day. Mot nguon su that duy nhat, khong co kenh phu nao.
    """
    return f"{DQ_ROOT}/decisions/{key}/{run_id}/{stage}.json"


def get_decision(key: str, run_id: str, stage: str):
    return read_json(decision_path(key, run_id, stage))


#: Trang thai cua mot ban ghi quyet dinh khi job van dang nam cho nguoi tra loi.
DECISION_PENDING = "pending"


def list_decisions(key: str) -> list:
    """MOI ban ghi quyet dinh cua mot pipeline - ca dang cho lan da tra loi.

    Ban ghi da tra loi khong bi xoa di dau ca: no van nam nguyen tren HDFS kem `decided_by`,
    `decided_reason`, `decided_at`. Nhung truoc day khong co duong nao doc ra, vi cho duy
    nhat duyet thu muc nay lai loc ngay `status == pending`. Ket qua la mot quyet dinh vua
    tra loi xong thi bien mat khoi giao dien, va khong ai tra lai duoc cau "lan truoc gap ca
    nay ta da chon gi".

    Moi nhat truoc, theo moc HOI chu khong phai moc tra loi: thu tu hoi la thu tu su viec
    xay ra.
    """
    import hdfs_client

    out = []
    for run_id in hdfs_client.list_dir(f"{DQ_ROOT}/decisions/{key}"):
        for name in hdfs_client.list_dir(f"{DQ_ROOT}/decisions/{key}/{run_id}"):
            if not name.endswith(".json"):
                continue
            doc = read_json(f"{DQ_ROOT}/decisions/{key}/{run_id}/{name}")
            if doc:
                doc.setdefault("dq_key", key)
                out.append(doc)
    out.sort(key=lambda d: d.get("requested_at") or d.get("asked_at") or "", reverse=True)
    return out


def list_pending_decisions(key: str) -> list:
    """Cac checkpoint dang CHO nguoi quyet dinh, moi nhat truoc.

    Chi DOC, va la mot bo LOC tren list_decisions chu khong tu duyet thu muc lan nua: hai cho
    cung duyet la hai cho co the lech nhau ve "the nao la dang cho", va mot cai cho bi bo quen
    nghia la job Spark treo ma khong ai biet.
    """
    return [d for d in list_decisions(key) if d.get("status") == DECISION_PENDING]


def checkpoints_path(key: str, run_id: str) -> str:
    return f"{DQ_ROOT}/checkpoints/{key}/{run_id}.json"


def get_checkpoints(key: str, run_id: str):
    return read_json(checkpoints_path(key, run_id))


#: Cau hinh qua DQ_TRANSCRIPT_MAX_CHARS (mac dinh 20000, 0 = khong cat).
MAX_TRANSCRIPT_CHARS = DQ_TRANSCRIPT_MAX_CHARS


def _cut(text: str) -> str:
    return text if MAX_TRANSCRIPT_CHARS <= 0 else text[:MAX_TRANSCRIPT_CHARS]


def _slim_transcript(transcript: list) -> list:
    """Giu dau vet tool call, cat phan cong kenh: ket qua tool co the vai tram KB."""
    out = []
    for t in transcript or []:
        result = t.get("result")
        text = result if isinstance(result, str) else json.dumps(result, ensure_ascii=False, default=str)
        out.append({
            "sequence_no": t.get("sequence_no"),
            "tool_name": t.get("tool_name"),
            "arguments_json": _cut(json.dumps(t.get("arguments"), ensure_ascii=False, default=str)),
            "result_json": _cut(text),
            "result_truncated": MAX_TRANSCRIPT_CHARS > 0 and len(text) > MAX_TRANSCRIPT_CHARS,
        })
    return out


# Cac ham GHI (publish_rules / record_checkpoint / save_analysis) da chuyen sang
# dq_store_write.py - module do tu choi import neu DQ_ROLE != "orchestrator". Module nay chi
# con duong dan va ham DOC, nen moi sub-agent import duoc ma khong the ghi.
