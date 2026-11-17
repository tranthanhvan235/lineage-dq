"""Ha tang dung chung cho moi pipeline: dia chi cluster, LLM, nguong mac dinh.

Tach khoi pipeline_config.py vi hai thu doi theo hai nhip khac nhau:
    pipeline_config.py  doi khi tro sang MOT BANG khac
    dq_env.py           doi khi tro sang MOT CUM khac

Doi ung phia Java: dq/DqConfig.java.
"""
import os
from pathlib import Path


def _load_dotenv():
    """Nap .env vao os.environ, KHONG ghi de bien da co san.

    Thu tu do co y: bien moi truong that luon thang. Trong cluster, SparkApplication va Secret
    dat bien cho driver - neu .env ghi de len chung thi cau hinh cluster se bi mot file nam
    trong image lam sai lech ma khong ai thay.

    Tim theo thu tu: DQ_ENV_FILE -> dq-agents/.env -> .env o thu muc cha.
    """
    ung_vien = []
    if os.getenv("DQ_ENV_FILE"):
        ung_vien.append(Path(os.environ["DQ_ENV_FILE"]))
    here = Path(__file__).resolve().parent
    ung_vien += [here / ".env", here.parent / ".env"]

    for f in ung_vien:
        if not f.is_file():
            continue
        for line in f.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, val = line.partition("=")
            key, val = key.strip(), val.strip().strip('"').strip("'")
            if key and key not in os.environ:
                os.environ[key] = val
        return f
    return None


DOTENV_FILE = _load_dotenv()


def _str(key: str, default: str) -> str:
    v = os.getenv(key)
    return v.strip() if v and v.strip() else default


def _int(key: str, default: int) -> int:
    try:
        return int(_str(key, str(default)))
    except ValueError:
        return default


def _float(key: str, default: float) -> float:
    try:
        return float(_str(key, str(default)))
    except ValueError:
        return default


# ── Khu vuc /dq/ tren HDFS ──────────────────────────────────────────────
DQ_ROOT = _str("DQ_ROOT", "/dq")
# Do dai toi da cua MOI ket qua tool khi ghi dau vet agent vao /dq/checkpoints (va bang
# dq_agent_tool_calls). 2000 cu cat mat gan het get_pipeline_context - tab Agent Log chi con
# doc duoc dong dau, khong truy duoc agent da thay gi. 0 = khong cat.
DQ_TRANSCRIPT_MAX_CHARS = _int("DQ_TRANSCRIPT_MAX_CHARS", 20000)

# ── HDFS ────────────────────────────────────────────────────────────────
WEBHDFS_URL = _str("WEBHDFS_URL", "http://localhost:9870")
HDFS_WEBHDFS_USER = _str("HDFS_WEBHDFS_USER", "root")
HDFS_NAMESPACE = _str("HDFS_NAMESPACE", "hdfs")
HDFS_DATANODE_SELECTOR = _str("HDFS_DATANODE_SELECTOR", "app=hdfs-datanode")
# Doi ten datanode trong redirect cua WebHDFS sang IP pod. Chi can tren MicroK8s, noi datanode
# la mot pod khong co Service. Cum co HDFS that (datanode co DNS) thi dat false: khong tat thi
# moi lan doc file deu thu 'microk8s kubectl' roi K8s API truoc khi chiu dung URL goc.
HDFS_DATANODE_REWRITE = _str("HDFS_DATANODE_REWRITE", "true").lower() not in (
    "0", "false", "no", "off")

# ── Marquez ─────────────────────────────────────────────────────────────
# Chi la duong lui cho lineage. Nguon chinh: /dq/lineage/{key}/latest.json (su kien goc).
MARQUEZ_URL = _str("MARQUEZ_URL", "http://localhost:30500")
# Namespace cua DATASET: chinh la URI kho vat ly (khac namespace cua JOB).
DATASET_NAMESPACE = _str("OPENLINEAGE_DATASET_NAMESPACE",
                         "hdfs://namenode:9000")
JOB_NAMESPACE = _str("OPENLINEAGE_NAMESPACE", "voice-call-pipeline")

# ── LLM ─────────────────────────────────────────────────────────────────
LLM_API_KEY = os.getenv("LLM_API_KEY") or os.getenv("GROQ_API_KEY")
LLM_BASE_URL = _str("LLM_BASE_URL", "https://api.groq.com/openai/v1")
LLM_MODEL = _str("LLM_MODEL", "llama-3.3-70b-versatile")
# 60 chu khong phai 20. Gen Rules phai di het MOI checkpoint va duyet SAU ho loi o tung
# checkpoint, nen chi phi vong lap ti le voi so checkpoint chu khong co dinh. Da do that tren
# pipeline 9 checkpoint: 20 vong khong du de phu het, 60 vong van cham tran khi agent phai sua
# lai sau khi bi cong do phu tu choi. Pipeline mot vai checkpoint thi 60 la du rong.
LLM_MAX_ITERATIONS = _int("LLM_MAX_ITERATIONS", 60)

# Khong dat thi mot lan goi LLM treo se giu tien trinh mai mai. Duong HTTP co runner canh
# deadline nen khong lo ra, nhung chay agent doc lap thi treo vo han - da dinh that, mot lan
# 57 phut khong nhuc nhich trong khi endpoint hoan toan khoe.
# Ten khong co tien to DQ_: runner.py dat dung hai ten nay khi cap ngan sach cho tung
# phien. Doc ten khac di la ngan sach khong bao gio toi agent.
LLM_REQUEST_TIMEOUT_SEC = _int("LLM_REQUEST_TIMEOUT_SEC", 180)
LLM_MAX_RETRIES = _int("LLM_MAX_RETRIES", 2)

# Gen Rules gom rule thanh lo. Duoi nguong nay thi nhac agent gom to hon: da do that
# 21/09/2026, agent chia lo 9-4-3-3-2 roi cham tran vong lap va mat trang ca bo rule.
SMALL_BATCH_THRESHOLD = _int("DQ_SMALL_BATCH_THRESHOLD", 5)
UNCOVERED_STAGES_SHOWN = _int("DQ_UNCOVERED_STAGES_SHOWN", 5)

# Tu nguong nay tro len, clear_accumulated_rules TU CHOI xoa. Mot bo 20 rule sai tu goc
# thi xoa la hop ly; mot bo 116 rule bi tu choi vi 5 rule thieu observed_unit thi xoa la
# vut ca luot chay. Sua tung rule bang add_rules (ghi de theo rule_id) la duong dung.
CLEAR_REFUSE_THRESHOLD = _int("DQ_CLEAR_REFUSE_THRESHOLD", 30)

# Tran do dai bieu thuc `transformation` cua MOT cot khi gui cho LLM.
#
# Bieu thuc nay do OpenLineage trai phang tu Catalyst, nen no khong ti le voi so cot ma voi
# do phuc tap phep bien doi. Da do that tren cell_capacity_daily: 38 cot -> 820 KB lineage
# = 205K token, rieng cot capacity_action 373 KB (mot CASE WHEN voi window long nhau). Prompt
# thanh 281K token > tran 262K cua model, va agent hong ngay loi goi LLM DAU TIEN - khong
# sinh noi mot rule nao.
#
# Cat CHI o duong gui cho LLM. Ban day du van nam tren HDFS va cac duong doc khac khong doi:
# agent can biet cot nay DUOC SUY TU dau va bang KIEU phep gi, khong can ca cay bieu thuc.
LINEAGE_EXPR_MAX_CHARS = _int("DQ_LINEAGE_EXPR_MAX_CHARS", 1200)

# ── Nguong sinh rule ────────────────────────────────────────────────────
VOLUME_ANOMALY_THRESHOLD_PCT = _float("DQ_VOLUME_ANOMALY_THRESHOLD_PCT", 30)
VOLUME_HISTORY_WINDOW = _int("DQ_VOLUME_HISTORY_WINDOW", 7)
PARTITION_MATCH_BUFFER_MS = _int("DQ_PARTITION_MATCH_BUFFER_MS", 10 * 60 * 1000)
PARTITION_COLUMN = _str("DQ_PARTITION_COLUMN", "date_time")
MAX_REGEN_ATTEMPTS = _int("DQ_MAX_REGEN_ATTEMPTS", 5)

# ── Kenh live voi job Spark ─────────────────────────────────────────────
# Do DqCheckpoint.java dat khi no mo kenh 2 chieu. Khong tu dat bien nay.
LIVE_CHANNEL = os.getenv("DQ_LIVE_CHANNEL") == "1"


def summary() -> dict:
    """Cau hinh ha tang, de in ra log hoac dua vao ngu canh cua Orchestrator."""
    return {
        "dotenv": str(DOTENV_FILE) if DOTENV_FILE else "(khong co)",
        "dq_root": DQ_ROOT,
        "webhdfs": WEBHDFS_URL,
        "marquez": MARQUEZ_URL + " (chi duong lui / visualize)",
        "dataset_namespace": DATASET_NAMESPACE,
        "llm": {"model": LLM_MODEL, "base_url": LLM_BASE_URL,
                "api_key": "co" if LLM_API_KEY else "THIEU"},
        "max_regen_attempts": MAX_REGEN_ATTEMPTS,
    }
