"""Per-pipeline settings — everything that changes when this framework is pointed at a new dataset.

Every value is env-overridable, so porting to a second pipeline means writing a .env (or a
ConfigMap), never editing code and never rebuilding the agent image.

Business metadata is looked up on HDFS by default ({DQ_ROOT}/metadata/{DQ_KEY}.yaml) for exactly
that reason: baking a per-pipeline YAML into the image would make the image pipeline-specific.
A local path still wins when it exists, so running from the repo is unchanged.

check() exists because the most dangerous misconfiguration here fails SILENTLY:
DqRuleLoader returns DqRuleSet.empty() when it cannot find rules, by design, so DQ must never
break the pipeline -- which also means a wrong key produces a clean, green run that checked
absolutely nothing.
"""
import os
from pathlib import Path

import yaml

from dq_env import DQ_ROOT
from hdfs_client import read_text

_MODULE_DIR = Path(__file__).resolve().parent

# The {key} in /dq/rules/{key}/, /dq/reports/{key}/, /dq/analysis/{key}/.
#
# No default on purpose. Inside a checkpoint the job sets both of these from its own output
# table (DqCheckpoint.exportEnv), so they are always present; running standalone means setting
# them in .env. A default here would be some other pipeline's table, and pointing DQ at the
# wrong table is the one failure that looks exactly like success -- a green run, 0 rules checked.
DQ_KEY = os.getenv("DQ_KEY", "")

# Physical dataset name, as it appears in the lineage event.
DATASET_NAME = os.getenv("DQ_DATASET_NAME", "")

# Business metadata YAML. Empty (the norm in-cluster) means HDFS at business_metadata_path().
# A value is tried as a local path first, then as an HDFS path.
BUSINESS_METADATA_YAML = os.getenv("DQ_BUSINESS_METADATA_YAML", "")


def business_metadata_path() -> str:
    """Where the YAML lives on HDFS when no local path is configured."""
    return f"{DQ_ROOT}/metadata/{DQ_KEY}.yaml"


def _resolve(path: str) -> Path:
    p = Path(path)
    return p if p.is_absolute() or p.exists() else _MODULE_DIR / path


def _sources(override: str = None) -> list:
    """(kind, location) to try, in order. Local before HDFS so repo runs keep working."""
    out = []
    for candidate in (override, BUSINESS_METADATA_YAML):
        if candidate:
            out.append(("local", str(_resolve(candidate))))
            if candidate.startswith("/"):
                out.append(("hdfs", candidate))
    out.append(("hdfs", business_metadata_path()))
    return out


def load_business_metadata(override: str = None) -> tuple:
    """(parsed YAML, where it came from). Raises FileNotFoundError if nothing resolves."""
    tried = []
    for kind, where in _sources(override):
        if kind == "local":
            if Path(where).is_file():
                return yaml.safe_load(Path(where).read_text(encoding="utf-8")) or {}, where
        else:
            raw = read_text(where)
            if raw:
                return yaml.safe_load(raw) or {}, f"hdfs:{where}"
        tried.append(where)
    raise FileNotFoundError("Khong tim thay business metadata YAML. Da thu: " + ", ".join(tried))


    return problems


def check() -> list:
    """Config problems that would make a run silently meaningless. Empty list == consistent."""
    problems = []

    if not DQ_KEY or not DATASET_NAME:
        return ["DQ_KEY hoac DQ_DATASET_NAME chua duoc dat. Trong checkpoint, job tu truyen hai "
                "bien nay tu ten bang dau ra cua no; chay doc lap thi dat trong .env."]

    derived = DATASET_NAME.rstrip("/").split("/")[-1]
    if derived != DQ_KEY:
        problems.append(
            f"DQ_KEY={DQ_KEY!r} != last segment of DQ_DATASET_NAME ({derived!r}). Python writes "
            f"rules to /dq/rules/{derived}/ while the Java job reads /dq/rules/<CLI arg 4>/ -- "
            "if those disagree the job finds no rules and reports a clean run that checked nothing."
        )

    try:
        meta, source = load_business_metadata()
    except Exception as e:
        problems.append(str(e))
    else:
        # Metadata cua pipeline CU van nam trong image, nen mot DQ_BUSINESS_METADATA_YAML sao
        # chep nham van tro toi mot file CO THAT - khong loi doc, chi la rule sinh ra cho sai
        # bang. Doi chieu dataset_name khai trong YAML voi DQ_KEY de chan dung truong hop do.
        declared = str(meta.get("dataset_name", "")).rstrip("/").split("/")[-1]
        if declared and declared != DQ_KEY:
            problems.append(
                f"Business metadata tai {source} khai dataset_name={declared!r} nhung DQ_KEY="
                f"{DQ_KEY!r} -- dang doc mo ta cua MOT BANG KHAC, rule sinh ra se sai bang."
            )

    return problems


def summary() -> dict:
    """Config as the Orchestrator should see it, including any problems."""
    return {
        "dq_key": DQ_KEY,
        "dataset_name": DATASET_NAME,
        "business_metadata_yaml": BUSINESS_METADATA_YAML or business_metadata_path(),
        "problems": check(),
    }
