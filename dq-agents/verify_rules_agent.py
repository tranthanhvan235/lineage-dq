"""Verify Rules agent — thẩm định bộ rule SAU khi Gen Rules sinh, TRƯỚC khi ghi xuống kho.

Vì sao tách hẳn thành một agent:
  Gen Rules trước đây gọi thẳng `publish_rules()` (gen_rules_agent.py:684) nên rule thành
  active mà không ai soi. Giờ Gen Rules chỉ `submit_rules_for_verification` — trả bộ rule về
  cho Orchestrator trong bộ nhớ, KHÔNG ghi gì. Chỉ agent này có `promote_rules`.

Thiết kế kiểm:
  Phần lớn là TẤT ĐỊNH (code), LLM chỉ phán phần code không phán được. Lý do: agent chạy với
  LLM_MAX_ITERATIONS=20; đã đo được Gen Rules thất bại 4/4 lần vì hết vòng lặp. Đẩy được việc
  gì sang code thì bộ verify còn chỗ để suy nghĩ phần khó.

  Tất định:
    P1 vị trí   — rule.column có ở manifest của đúng rule.stage không
    P2 kiểu     — TRIM()/RLIKE trên cột số, so sánh số với chuỗi (đã gặp thật: side, call_id
                  kiểu integer nhưng rule dùng TRIM(side) = '')
    P3 nhập nhằng — cột trùng tên trong stage đó (jn_map2 có imsi/msisdn lặp 2 lần)
    P4 chạy được — cú pháp, bẫy NULL, aggregate lồng aggregate, token cấm
    P5 độ phủ   — cột nào ở mỗi stage chưa rule nào chạm tới

  LLM phán:
    - rule CHẾT do phép biến đổi (IS NULL sau coalesce → không bao giờ bắn)
    - rule đặt sai chỗ về mặt nghiệp vụ (đặt được sớm hơn mà lại đặt muộn)
    - verdict cuối từng stage: PASS | WEAK | FAIL

Verdict đi kèm generation_run_id và được ghi ra /dq/rules/{key}/verification/{gen_run_id}.json.
Phía Java (DqRuleLoader) TỪ CHỐI nạp rule set không có verdict khớp — đó mới là chỗ khiến việc
từ chối có hiệu lực thật, chứ tầng Python chỉ là quy ước.
"""
import json
import re
from datetime import datetime, timezone

from agent_loop import AgentLoopError, run_tool_calling_agent
from skill_prompt_loader import load_skill_docs
from tool_registry import ToolRegistry

DEFAULT_STAGE = "final"

# Kiểu số: TRIM/RLIKE/so sánh chuỗi trên các kiểu này là rule không bao giờ bắn.
_NUMERIC = ("int", "bigint", "smallint", "tinyint", "double", "float", "decimal", "long")
_STRINGY_FN = re.compile(r"\b(TRIM|LTRIM|RTRIM|UPPER|LOWER|RLIKE|LIKE|SUBSTRING|SUBSTR|CONCAT)\s*\(",
                         re.IGNORECASE)
_AGG_FN = re.compile(r"\b(COUNT|SUM|AVG|MIN|MAX|STDDEV|VARIANCE|COLLECT_LIST|COLLECT_SET)\s*\(",
                     re.IGNORECASE)


#: Don vi cua observed_expr - phai trung DqRule.UNIT_*. Quyet dinh cach quy doi rule
#: muc tap ra ty le vi pham, nen khai sai la diem sai.
OBSERVED_UNITS = ("rows", "percent", "value")


def mentions_null(expr: str) -> bool:
    """Bieu thuc co xu ly NULL mot cach tuong minh chua - theo huong nao cung duoc.

    Ban sao cua gen_rules_agent.mentions_null. Hai agent chay hai tien trinh khac nhau nen
    khong dung chung bien; test_skill_docs_consistent.py doi chieu hai ban tren cung mot bo
    bieu thuc mau va bao dong neu lech - cung cach OBSERVED_UNITS dang lam.
    """
    return "IS NULL" in expr.upper() or "IS NOT NULL" in expr.upper()

def _stage_index(stage_manifest):
    """{stage: {"cols": {name: type}, "dups": set(), "order": n, "rows": n}}"""
    out = {}
    for st in (stage_manifest or {}).get("stages", []):
        name = st.get("stage")
        if not name:
            continue
        cols = {}
        for c in st.get("schema") or []:
            cols[c.get("name")] = c.get("type", "")
        if not cols:
            for n in st.get("columns") or []:
                cols[n] = ""
        out[name] = {
            "cols": cols,
            "dups": set(st.get("duplicate_names") or []),
            "order": st.get("order"),
            "rows": st.get("row_count"),
        }
    return out


def deterministic_checks(rules, stages):
    """Trả (findings_by_stage, summary). Không gọi LLM. Không bao giờ ném."""
    by_stage = {}
    seen_ids = set()

    for i, r in enumerate(rules):
        stage = r.get("stage") or DEFAULT_STAGE
        slot = by_stage.setdefault(stage, {"misplaced": [], "type_mismatch": [], "ambiguous": [],
                                           "unexecutable": [], "duplicate": [], "rule_ids": []})
        rid = r.get("rule_id", f"(rule[{i}])")
        slot["rule_ids"].append(rid)
        expr = str(r.get("failed_sql_expr", ""))
        col = str(r.get("column", "")).strip()
        info = stages.get(stage)

        key = (stage, rid)
        if key in seen_ids:
            slot["duplicate"].append(f"{rid}: trùng (stage, rule_id)")
        seen_ids.add(key)

        # P1 vị trí
        if info is None:
            slot["misplaced"].append(f"{rid}: stage '{stage}' KHÔNG có trong manifest")
        elif col and col not in info["cols"]:
            slot["misplaced"].append(
                f"{rid}: cột '{col}' không có ở stage này (có {len(info['cols'])} cột)")
        else:
            # P2 kiểu
            t = (info["cols"].get(col) or "").lower()
            if any(t.startswith(n) for n in _NUMERIC) and _STRINGY_FN.search(expr):
                slot["type_mismatch"].append(
                    f"{rid}: cột '{col}' kiểu {t} nhưng biểu thức dùng hàm chuỗi -> không bao giờ bắn")
            # P3 nhập nhằng
            if col in info["dups"]:
                slot["ambiguous"].append(
                    f"{rid}: cột '{col}' xuất hiện NHIỀU LẦN ở stage này -> AMBIGUOUS_REFERENCE")

        # P4 chạy được
        if expr.count("(") != expr.count(")"):
            slot["unexecutable"].append(f"{rid}: ngoặc không cân bằng")
        # Bẫy NULL chỉ bắt buộc với biểu thức MỨC DÒNG. Rule mức tập là một aggregate trả về
        # boolean (count(distinct k) <> count(k)) — ép nó chứa "IS NULL" là vô nghĩa.
        if r.get("kind") != "dataset" and not mentions_null(expr):
            slot["unexecutable"].append(
                f"{rid}: khong noi ro y dinh voi NULL (can 'IS NULL' hoac 'IS NOT NULL')")
        if r.get("kind") == "dataset" and r.get("observed_unit") not in OBSERVED_UNITS:
            slot["unexecutable"].append(
                f"{rid}: rule mức tập thiếu observed_unit hợp lệ ({', '.join(OBSERVED_UNITS)})"
                " -> điểm sẽ coi mọi vi phạm là hỏng 100%")
        if r.get("kind") != "dataset" and _AGG_FN.search(expr):
            slot["unexecutable"].append(
                f"{rid}: có aggregate trong biểu thức mức dòng -> NESTED_AGGREGATES_UNSUPPORTED"
                " (nếu cố ý kiểm mức tập thì phải đặt kind='dataset')")
        try:
            w = int(r.get("importance_weight", 0))
            if not 1 <= w <= 5:
                slot["unexecutable"].append(f"{rid}: importance_weight={w} ngoài [1..5]")
        except (TypeError, ValueError):
            slot["unexecutable"].append(f"{rid}: importance_weight không phải số nguyên")

    # P5 độ phủ — tính cho MỌI stage trong manifest, kể cả stage không có rule nào
    for stage, info in stages.items():
        slot = by_stage.setdefault(stage, {"misplaced": [], "type_mismatch": [], "ambiguous": [],
                                           "unexecutable": [], "duplicate": [], "rule_ids": []})
        covered = {r.get("column") for r in rules if (r.get("stage") or DEFAULT_STAGE) == stage}
        slot["uncovered_columns"] = sorted(set(info["cols"]) - covered)
        slot["rule_count"] = len(slot["rule_ids"])
        slot["column_count"] = len(info["cols"])

    summary = {
        "stages_in_manifest": len(stages),
        "stages_with_rules": sum(1 for v in by_stage.values() if v.get("rule_count")),
        "total_rules": len(rules),
        "hard_problems": sum(len(v["misplaced"]) + len(v["type_mismatch"]) + len(v["ambiguous"])
                             + len(v["unexecutable"]) + len(v["duplicate"]) for v in by_stage.values()),
    }
    return by_stage, summary


#: Ban rut gon, CHI dung khi khong doc duoc skills/dq-rule-verifier. Nguon su that la skill
#: docs; giu ban nay ngan va khong mau thuan voi chung, dung mo rong no o day.
SYSTEM_PROMPT_FALLBACK = """Bạn là Verify Rules agent. Việc của bạn là THẨM ĐỊNH một bộ rule Data Quality
đã được sinh ra nhưng CHƯA được lưu, rồi quyết định có cho lưu hay không.

Phần kiểm máy móc (vị trí cột, kiểu dữ liệu, cú pháp, độ phủ) ĐÃ CHẠY XONG và kết quả nằm trong
context. Đừng làm lại. Việc của bạn là phần máy không phán được:

1. RULE CHẾT vì phép biến đổi: rule đúng cú pháp nhưng không bao giờ bắn được. Dạng hay gặp
   nhất là kiểm NULL trên cột đã qua một phép điền giá trị mặc định — sau coalesce thì cột
   không bao giờ NULL nữa, nên rule đó chết vĩnh viễn; muốn bắt lỗi phải kiểm CHÍNH giá trị
   mặc định đó, hoặc kiểm cột nguồn ở checkpoint TRƯỚC phép biến đổi.
   LƯU Ý GIỚI HẠN: bạn KHÔNG được cấp column lineage, nên chỉ nêu nghi vấn khi mô tả của rule
   hoặc chính biểu thức đã lộ ra phép biến đổi. Đừng suy đoán phép biến đổi từ tên cột.
2. ĐẶT SAI CHỖ về nghiệp vụ: rule đặt được ở checkpoint sớm hơn mà lại đặt muộn. Bắt lỗi ở nguồn
   rẻ hơn và chỉ đúng thủ phạm hơn.
3. Verdict cho TỪNG stage: PASS | WEAK | FAIL.
   - FAIL: có lỗi cứng (đặt sai stage, nhập nhằng, không chạy được) hoặc bỏ trống cột trọng yếu
   - WEAK: chạy được nhưng phủ mỏng hoặc có rule đáng ngờ
   - PASS: dùng được

KẾT THÚC bằng đúng một trong hai:
- submit_verdict(overall, per_stage, notes): ĐẠT. overall phải là PASS hoặc WEAK.
- reject_rules(reason, must_fix): TỪ CHỐI. Gen Rules sẽ sinh lại.

Bạn KHÔNG ghi gì xuống kho. Bạn chỉ ra phán quyết; Orchestrator là bên duy nhất có quyền ghi.

Gọi ít vòng thôi — bạn chỉ có 20 vòng lặp."""


def build_system_prompt() -> str:
    """Skill docs la nguon su that; hang so tren chi la luoi do khi khong doc duoc thu muc.

    Cung khuon voi gen_rules_agent.build_system_prompt. Tach ra thanh ham chu khong noi thang
    o run() de test_skill_docs_consistent goi duoc ma khong phai dung toi LLM.
    """
    return load_skill_docs(["dq-rule-verifier"]) or SYSTEM_PROMPT_FALLBACK


def run(dq_key, dataset_name, candidate, stage_manifest):
    """candidate: bản ghi Gen Rules nộp lên (chưa lưu). Trả dict kết quả cho Orchestrator."""
    rules = candidate.get("rules", [])
    generation_run_id = candidate.get("generation_run_id")
    stages = _stage_index(stage_manifest)
    findings, summary = deterministic_checks(rules, stages)

    state = {"done": None}
    tools = ToolRegistry()

    @tools.tool("Ra phan quyet DAT cho bo rule. overall=PASS|WEAK. per_stage la dict "
                "{stage: PASS|WEAK|FAIL}. KHONG ghi gi ca - Orchestrator se ghi. Tool KET THUC.")
    def submit_verdict(overall: str, per_stage: dict = None, notes: str = ""):
        if overall not in ("PASS", "WEAK"):
            return {"error": "overall phải là PASS hoặc WEAK. Muốn từ chối thì gọi reject_rules."}
        verdict_doc = {
            "dq_key": dq_key,
            "dataset_name": dataset_name,
            "generation_run_id": generation_run_id,
            "verdict": overall,
            "per_stage": per_stage or {},
            "notes": notes,
            "deterministic": findings,
            "summary": summary,
            "verified_at": datetime.now(timezone.utc).isoformat(),
        }
        # KHONG ghi. Agent nay chi tham dinh. Orchestrator doc verdict_doc roi tu ghi theo
        # dung thu tu verdict -> history -> active (dq_store_write.promote).
        state["done"] = {"approved": True, "verdict": overall, "verdict_doc": verdict_doc}
        return {"approved": True, "verdict": overall,
                "note": "Da ra phan quyet. Orchestrator se ghi xuong kho."}

    @tools.tool("TỪ CHỐI bộ rule - không lưu gì cả. must_fix là danh sách việc Gen Rules phải "
                "sửa. Tool KẾT THÚC.")
    def reject_rules(reason: str, must_fix: list = None):
        state["done"] = {"approved": False, "reason": reason, "must_fix": must_fix or [],
                         "deterministic": findings}
        return {"approved": False, "reason": reason}

    context = f"""=== BỘ RULE CHỜ THẨM ĐỊNH ===
dataset: {dataset_name}
generation_run_id: {generation_run_id}
tổng: {summary['total_rules']} rule / {summary['stages_in_manifest']} checkpoint
(có rule ở {summary['stages_with_rules']} checkpoint; {summary['hard_problems']} lỗi cứng máy đã bắt)

=== KẾT QUẢ KIỂM MÁY MÓC (đã chạy, đừng làm lại) ===
{json.dumps(findings, ensure_ascii=False, indent=2)}

=== TOÀN BỘ RULE ===
{json.dumps(rules, ensure_ascii=False, indent=2)}
"""
    try:
        stop_tool, stop_result, _ = run_tool_calling_agent(
            system_prompt=build_system_prompt(),
            user_prompt=context,
            tools_schema=tools.schema(),
            tool_dispatch=tools.dispatch(),
            stop_tools={"submit_verdict", "reject_rules"},
        )
    except AgentLoopError as e:
        # Hết vòng lặp mà chưa quyết -> KHÔNG lưu. Mặc định an toàn là từ chối: thà chạy với bộ
        # rule cũ đã từng qua thẩm định còn hơn lưu bộ chưa ai soi.
        return {"approved": False, "reason": f"Verify agent không kết thúc: {e}",
                "deterministic": findings, "summary": summary}

    return state["done"] or {"approved": False, "reason": f"stop_tool={stop_tool}",
                             "detail": stop_result, "deterministic": findings}
