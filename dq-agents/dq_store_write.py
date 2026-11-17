"""Moi thao tac GHI vao kho /dq/. Chi Orchestrator duoc import module nay.

Vi sao tach khoi dq_store: thiet ke quy dinh Orchestrator la ben DUY NHAT giao tiep voi kho
luu tru, con ba sub-agent (gen / verify / analysis) chi la ham thuan - nhan du lieu, tra dict.
Truoc khi tach, quy dinh do chi ton tai trong loi dan cua prompt, va thuc te ca ba deu ghi
thang: verify_rules_agent goi publish_rules + write_json, analysis_agent goi save_analysis.
Mot loi dan trong prompt khong chan duoc dieu do; mot lenh import thi co.

Chot bang bien moi truong DQ_ROLE chu khong bang quy uoc dat ten: tien trinh con do
runner._child_env / DqCheckpoint sinh ra deu KHONG dat bien nay, nen mot sub-agent lo import
nham se hong NGAY luc import, o may phat trien, chu khong phai am tham ghi de kho tren cum.
"""
import os

from hdfs_client import write_json

from dq_store import (analysis_history_path, analysis_latest_path, checkpoints_path,
                      decision_path,
                      read_json, rules_active_path, rules_history_path, verification_path,
                      _slim_transcript)

ORCHESTRATOR_ROLE = "orchestrator"
_role = os.getenv("DQ_ROLE", "")
if _role != ORCHESTRATOR_ROLE:
    raise ImportError(
        f"dq_store_write is orchestrator-only (needs DQ_ROLE={ORCHESTRATOR_ROLE}, got "
        f"'{_role or 'unset'}'). Sub-agents must not write to the store directly - return the "
        "result to the Orchestrator and let it persist."
    )


def publish_verdict(key: str, generation_run_id: str, verdict_doc: dict) -> None:
    write_json(verification_path(key, generation_run_id), verdict_doc)


def publish_rules(key: str, record: dict, run_id: str) -> None:
    write_json(rules_history_path(key, run_id), record)
    write_json(rules_active_path(key), record)


def promote(key: str, candidate: dict, generation_run_id: str, verdict_doc: dict) -> None:
    """Ghi mot bo rule da tham dinh DAT, theo dung thu tu bat buoc.

    verdict -> history -> active. Chet may o bat ky khe nao cung khong de lai mot active.json
    thieu verdict: DqRuleLoader.verified() doc verification/{generation_run_id}.json va TU CHOI
    nap rule set khong co verdict khop. Dao thu tu lai thi co mot khoang thoi gian active.json
    da doi ma verdict chua co, va dung luc do job doc thi no chay voi 0 rule.
    """
    publish_verdict(key, generation_run_id, verdict_doc)
    publish_rules(key, candidate, generation_run_id)


def request_decision(key: str, run_id: str, stage: str, record: dict) -> str:
    """Ghi ban ghi CHO QUYET DINH va tra ve duong dan de bao cho nguoi biet.

    status = "pending" la tin hieu duy nhat Java doi: vong cho ben do (DqHumanDecision) doc lap
    file nay cho toi khi truong do khac "pending". Ghi xong la TRA VE NGAY - tuyet doi khong
    chan o day, vi tien trinh nay la con cua phien agent, ma phien agent co chot
    HTTP_MAX_NO_PROGRESS se giet no sau ~66 phut khong goi tool nao.
    """
    path = decision_path(key, run_id, stage)
    write_json(path, {**record, "status": "pending"})
    return path


def answer_decision(key: str, run_id: str, stage: str, action: str, who: str,
                    reason: str = "") -> dict:
    """Ghi cau tra loi cua nguoi de nha vong cho ben Java.

    Doc lai ban ghi cho roi ghi de, thay vi tao moi: giu nguyen ngu canh (diem, rule vi pham,
    phan tich) de sau con doi chieu duoc "nguoi thay gi khi quyet dinh".
    """
    path = decision_path(key, run_id, stage)
    current = read_json(path) or {}
    if not current:
        raise FileNotFoundError(f"Khong co ban ghi cho quyet dinh tai {path}"
                                f" - job co dang thuc su cho khong?")
    if current.get("status") != "pending":
        raise ValueError(f"Quyet dinh nay da duoc tra loi roi: {current.get('status')}"
                         f" (boi {current.get('decided_by')})")
    answered = {**current, "status": action, "decided_by": who,
                "decided_reason": reason, "decided_at": _now()}
    write_json(path, answered)
    return answered


def _now() -> str:
    from datetime import datetime, timezone
    return datetime.now(timezone.utc).isoformat()


def save_analysis(key: str, record: dict, analysis_id: str) -> None:
    write_json(analysis_latest_path(key), record)
    write_json(analysis_history_path(key, analysis_id), record)


def record_checkpoint(key: str, run_id: str, checkpoint: str, decision: dict,
                      transcript: list = None, attempts: list = None) -> None:
    """Ghi them mot quyet dinh checkpoint cua mot lan chay Spark.

    Khoa theo run_id de quyet dinh nam canh report mang cung id. Khong co no thi dau vet duy
    nhat la log driver, ma log driver thi roi theo pod.
    """
    doc = read_json(checkpoints_path(key, run_id)) or {"dq_key": key, "run_id": run_id,
                                                       "checkpoints": []}
    # Duong nao da sinh ra quyet dinh nay. Khong co no thi mot checkpoint di qua HTTP va mot
    # checkpoint di qua ProcessBuilder trong Y HET NHAU tren HDFS.
    entry = {"checkpoint": checkpoint,
             "transport": os.getenv("DQ_CHECKPOINT_TRANSPORT", "process"),
             **decision}
    if transcript:
        entry["tool_calls"] = _slim_transcript(transcript)
    if attempts:
        entry["attempts"] = attempts
    doc["checkpoints"].append(entry)
    write_json(checkpoints_path(key, run_id), doc)
