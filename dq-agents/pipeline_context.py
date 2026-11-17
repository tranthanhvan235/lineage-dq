"""Compact pipeline-context digest for the Orchestrator.

The Orchestrator is the one agent that must survive a long multi-turn conversation, so it must
NOT receive the payloads the subagents work on: Gen Rules alone dumps the entire business YAML
plus the full lineage JSON into its prompt. Instead it gets this digest -- counts, names, status
and one changed/not-changed flag -- which is enough to DECIDE which subagent to call, while each
subagent still fetches the full payload it needs to WORK.

Every section degrades independently: a section that cannot be read comes back with
available=False plus a reason, never an exception, so the Orchestrator always gets a usable
picture even when Marquez or HDFS is down.
"""
import pipeline_config
from dq_store import get_active_rules, get_latest_report, get_stages, get_verification
from lineage_client import get_column_lineage

def _lineage_section(dataset_name: str) -> dict:
    try:
        ctx = dict(get_column_lineage(dataset_name))
    except Exception as e:  # degrade, never break the Orchestrator's turn
        return {"available": False, "reason": str(e)}

    indirect = ctx.pop("__indirect_impact__", []) or []
    if not ctx:
        return {"available": False, "reason": "Marquez chua co column lineage cho dataset nay "
                                               "(job da chay lan nao chua?)"}
    return {
        "available": True,
        "column_count": len(ctx),
        "columns": sorted(ctx.keys()),
        "indirect_deps": [{"source": d.get("source"), "subtype": d.get("subtype")} for d in indirect],
    }


def _business_metadata_section() -> dict:
    try:
        meta, source = pipeline_config.load_business_metadata()
    except Exception as e:
        return {"available": False, "reason": str(e)}

    columns = meta.get("columns") or {}
    if isinstance(columns, dict):
        column_names = sorted(columns.keys())
    elif isinstance(columns, list):
        column_names = [c.get("name") for c in columns if isinstance(c, dict) and c.get("name")]
    else:
        column_names = []

    constants = meta.get("domain_constants") or {}
    return {
        "available": True,
        "path": source,
        "description": str(meta.get("description", ""))[:300],
        "described_column_count": len(column_names),
        "described_columns": column_names,
        "domain_constants": sorted(constants.keys()) if isinstance(constants, dict) else [],
        "has_reference_sql": bool(meta.get("reference_sql")),
    }


def _rules_section(dq_key: str) -> dict:
    try:
        rule_set = get_active_rules(dq_key)
    except Exception as e:
        return {"exists": False, "reason": str(e)}
    if not rule_set or not rule_set.get("rules"):
        return {"exists": False, "reason": "Chua co rule active - can chay Gen Rules truoc."}

    rules = rule_set["rules"]
    return {
        "exists": True,
        "count": len(rules),
        "generation_run_id": rule_set.get("generation_run_id"),
        "generated_at": rule_set.get("generated_at"),
        "lineage_hash": rule_set.get("lineage_hash"),
        "columns_covered": sorted({r.get("column") for r in rules if r.get("column")}),
    }


def _report_section(dq_key: str) -> dict:
    try:
        report = get_latest_report(dq_key)
    except Exception as e:
        return {"exists": False, "reason": str(e)}
    if not report:
        return {"exists": False, "reason": "Chua co report - job Spark chua chay lan nao."}

    # Report dang per_stage: cap tai lieu CHI co worst_status/stage_count, diem va rule nam
    # trong tung stages[]. Doc theo dang cu thi agent duoc bao "lan chay truoc kiem 0 rule,
    # status None" - mot lan chay hoan toan binh thuong trong y het mot lan chay hong.
    per_stage = report.get("report_shape") == "per_stage"
    stages = sorted(report.get("stages") or [],
                    key=lambda x: x.get("order") if x.get("order") is not None else 0)
    if per_stage:
        rules = [r for st in stages for r in (st.get("rules") or [])]
        scores = [st.get("quality_score") for st in stages
                  if st.get("quality_score") is not None]
        status = report.get("worst_status")
        # Diem cua checkpoint TE NHAT. Khong co diem tong - moi checkpoint mau so rieng.
        worst_score = min(scores) if scores else None
        total_rows = stages[-1].get("total_rows") if stages else None
    else:
        rules = report.get("rules", [])
        status = report.get("status")
        worst_score = report.get("quality_score")
        total_rows = report.get("total_rows")

    return {
        "exists": True,
        "run_id": report.get("run_id"),
        "executed_at": report.get("executed_at"),
        "status": status,
        "worst_stage_score": worst_score,
        "total_rows": total_rows,
        "stage_count": len(stages) if per_stage else None,
        "per_stage": [{"stage": st.get("stage"), "status": st.get("status"),
                       "quality_score": st.get("quality_score"),
                       "total_rows": st.get("total_rows"),
                       "rules_violated": sum(1 for r in (st.get("rules") or [])
                                             if (r.get("violation_count") or 0) > 0)}
                      for st in stages],
        "rules_checked": len(rules),
        "rules_violated": sum(1 for r in rules if r.get("violation_count", 0) > 0),
        "checked_against_generation_run_id": report.get("checked_against_generation_run_id"),
    }


def _lineage_changed(dataset_name: str, rules: dict):
    """True if lineage moved since the active rule set was generated.

    Reuses Gen Rules' own hash function on purpose - a second implementation here would drift
    and make the comparison silently meaningless. Returns None when it cannot be determined.
    """
    stored = rules.get("lineage_hash")
    if not stored:
        return None
    try:
        from gen_rules_agent import _compute_lineage_hash
        return _compute_lineage_hash(dataset_name) != stored
    except Exception:
        return None


def _checkpoint_section(dq_key: str, rules: dict) -> dict:
    """Bo rule hien tai co con khop voi cac checkpoint dev dat trong code khong.

    Day la tin hieu DUY NHAT cho Orchestrator biet phai sinh lai rule khi dev them/doi
    checkpoint. Truoc day chi co lineage_changed_since_rules, ma them mot checkpoint moi
    KHONG lam doi lineage - nen rule cu o nguyen day va cac checkpoint moi chay voi 0 rule
    ma khong ai bao gi.
    """
    try:
        manifest = get_stages(dq_key) or {}
    except Exception as e:
        return {"known": False, "reason": str(e)}
    stages = [s.get("stage") for s in manifest.get("stages", []) if s.get("stage")]
    if not stages:
        return {"known": False,
                "reason": "Chua co manifest checkpoint - pipeline chua chay lan nao voi gate."}

    # Doc lai rule tho o day chu khong nhan qua digest: digest di thang vao context cua LLM,
    # nhet 79 rule vao do la phi token va co the day agent cham tran vong lap.
    try:
        rule_set = get_active_rules(dq_key) or {}
    except Exception:
        rule_set = {}
    covered = {}
    for r in rule_set.get("rules") or []:
        st = r.get("stage") or "final"
        covered[st] = covered.get(st, 0) + 1
    missing = [s for s in stages if not covered.get(s)]
    return {
        "known": True,
        "stages_in_code": stages,
        "stages_with_rules": sorted(covered),
        "stages_WITHOUT_rules": missing,
        "needs_regeneration": bool(missing),
        "why": (f"{len(missing)}/{len(stages)} checkpoint chua co rule nao: {', '.join(missing)}"
                if missing else "moi checkpoint deu co rule"),
    }


def _verification_section(dq_key: str, rules: dict) -> dict:
    """Bo rule active da qua tham dinh chua. Java TU CHOI nap neu chua (DqRuleLoader)."""
    gen = rules.get("generation_run_id")
    if not gen:
        return {"verified": False, "reason": "rule set khong co generation_run_id"}
    try:
        v = get_verification(dq_key, gen)
    except Exception as e:
        return {"verified": False, "reason": str(e)}
    if not v:
        return {"verified": False,
                "reason": f"KHONG co verdict cho generation_run_id={gen}. Java se tu choi nap"
                          " bo rule nay -> pipeline chay voi 0 rule. Phai sinh lai va tham dinh."}
    return {"verified": v.get("verdict") in ("PASS", "WEAK"), "verdict": v.get("verdict"),
            "per_stage": v.get("per_stage"), "verified_at": v.get("verified_at")}


def build_digest(dq_key: str, dataset_name: str) -> dict:
    """Small, decision-oriented snapshot of the pipeline. Safe to keep in the Orchestrator's
    context across a whole conversation.
    """
    rules = _rules_section(dq_key)
    digest = {
        "dq_key": dq_key,
        "dataset_name": dataset_name,
        # Misconfiguration here is the one failure that looks like success (a green run that
        # checked nothing), so it is surfaced to the Orchestrator rather than left to a log.
        "config_problems": pipeline_config.check(),
        "lineage": _lineage_section(dataset_name),
        "business_metadata": _business_metadata_section(),
        "active_rules": rules,
        "last_report": _report_section(dq_key),
    }
    digest["lineage_changed_since_rules"] = (
        _lineage_changed(dataset_name, rules) if rules.get("exists") else None
    )
    digest["checkpoint_coverage"] = _checkpoint_section(dq_key, rules)
    digest["rules_verification"] = (
        _verification_section(dq_key, rules) if rules.get("exists")
        else {"verified": False, "reason": "chua co rule"}
    )
    return digest
