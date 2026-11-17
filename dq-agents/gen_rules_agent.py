"""
Gen Rules agent (tool-calling, real agent — LLM decides which tool to call and when).

Ported from dq-lineage-pipeline/jobs/dq_core/agent_gen_rules.py + gen_rules.py, adapted:
  - No Postgres: rules/lineage-hash state persisted as local JSON under state/ instead of
    dq_rules / dq_lineage_state tables. Same "deactivate old, write new" semantics, file form.
  - No DuckDB/MinIO dry-run: this pipeline is Iceberg-on-HDFS via Hive metastore, not
    Delta-on-MinIO. dry_run_rules is kept as a tool for interface completeness but honestly
    reports "not available for this pipeline" instead of silently no-op'ing — the two other
    self-check layers (verify_accumulated_rules, check_rules_against_schema) still run.
  - Business metadata is a local YAML path, not fetched from elsewhere.
  - get_downstream_datasets always returns [] here (see lineage_client.py docstring).

This is the "Gen Rules (with skill + self-check)" subagent from the multi-agent design —
runnable standalone (no orchestrator yet) as the first demo milestone.
"""
import hashlib
import json
import os
import re
import sys
import uuid
from datetime import datetime, timezone

import yaml

import pipeline_config

from agent_loop import AgentLoopError, run_tool_calling_agent
import dq_env as _env
from dq_store import get_active_rules, rules_active_path
from hdfs_client import list_partition_values
from lineage_client import get_column_lineage, get_dataset_schema, get_downstream_datasets, get_row_count_history
from skill_prompt_loader import load_skill_docs

def _dq_key(dataset_name: str) -> str:
    """Segment cuoi cua dataset_name - khoa thu muc trong /dq/. Phai khop cfg.getOutTable() phia
    Java, xem pipeline_config.check()."""
    return dataset_name.rstrip("/").split("/")[-1]

FORBIDDEN_SQL_TOKENS = (";", "--", "DROP ", "DELETE ", "INSERT ", "UPDATE ", "ALTER ", "TRUNCATE ")

_SQL_NON_COLUMN_TOKENS = {
    "and", "or", "not", "is", "null", "in", "between", "like", "rlike", "case", "when",
    "then", "else", "end", "cast", "as", "true", "false", "select", "from", "where",
    "distinct", "exists", "asc", "desc", "interval", "current_date", "current_timestamp",
    "int", "integer", "bigint", "smallint", "double", "float", "string", "boolean",
    "decimal", "date", "timestamp", "long", "short",
}
_IDENT_RE = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")

OUTPUT_INVARIANTS = """
=== YÊU CẦU TRẢ VỀ (BẮT BUỘC – KHÔNG ĐƯỢC VI PHẠM) ===
Trả về JSON NGUYÊN CHẤT (không markdown, không code fence) với đúng 2 trường chính:
1. "overall_impact_warning": Cảnh báo 1-2 câu về hậu quả hạ nguồn nếu dữ liệu này sai.
2. "rules": Danh sách các luật. Mỗi luật gồm:
   - "stage": Tên checkpoint sẽ đo luật này — phải có thật trong mục 0.
   - "rule_id": Mã luật (VD: RSRP_OUT_OF_BOUNDS)
   - "column": Tên cột ĐẦU VÀO bị kiểm tra
   - "description": Mô tả chi tiết logic lỗi
   - "failed_sql_expr": Biểu thức SQL INVERTED LOGIC, tương thích Spark SQL 3.x.
   - "importance_weight": Số nguyên trong [1, 5] do AI tự đánh giá.
   - "kind": "row" (mặc định) hoặc "dataset". Quyết định cách viết failed_sql_expr:
       row     -> biểu thức MỨC DÒNG, PHẢI nói rõ ý định với NULL, cấm aggregate.
                  Hai cách nói, chọn đúng một: "col IS NULL" nếu NULL LÀ vi phạm, hoặc
                  "col IS NOT NULL AND <kiểm miền giá trị>" nếu NULL là HỢP LỆ ở cột này.
       dataset -> biểu thức LÀ aggregate trả boolean (vd count(DISTINCT k) <> count(k)),
                  KHÔNG cần bẫy NULL, và BẮT BUỘC kèm "observed_expr" + "expected".
   - "observed_expr", "expected", "observed_unit": chỉ cho kind="dataset".
     "observed_unit" quyết định điểm: "rows" (observed_expr trả SỐ DÒNG vi phạm) hoặc
     "percent" (đã là 0-100) thì điểm phản ánh đúng mức độ hỏng; "value" hoặc bỏ trống thì
     mọi vi phạm đều bị tính là hỏng 100%. Rule kiểm khoá duy nhất dùng
     observed_expr = count(k) - count(DISTINCT k), nên observed_unit = "rows".

=== ĐỦ SÁU HỌ LỖI (BẮT BUỘC) ===
Với MỖI checkpoint, duyệt đủ sáu họ: completeness, validity, uniqueness, referential,
consistency, distribution (METHODOLOGY.md mục 2.8). Họ nào không sinh rule thì nêu lý do
trong overall_impact_warning — không được im lặng bỏ qua.

Hai họ uniqueness và distribution CHỈ viết được bằng kind="dataset". Một bộ rule không có
rule kind="dataset" nào là bộ rule đã bỏ sót hai họ, và sẽ bị trả lại.
"""

AGENT_ORCHESTRATION_INSTRUCTIONS = """

=== VAI TRÒ AGENT (Tool-Calling — bạn TOÀN QUYỀN quyết định) ===
Bạn là một AGENT THẬT, tự quyết định gọi tool nào và theo thứ tự nào. Các tool khả dụng:
- count_active_rules(): xem hiện có bao nhiêu rule đang active cho dataset này (lần chạy trước).
- check_lineage_changed(): xem lineage cột (nguồn gốc + phép biến đổi) có đổi từ lần sinh rule trước không.
- check_volume_anomaly(): xem row-count (input/output) của lần chạy gần nhất có tăng/giảm bất thường so với
  lần chạy trước không (dựa trên facet OpenLineage, không tốn chi phí query Spark). NÊN gọi tool này trước khi
  add_rule: nếu có anomaly, hãy phản ánh vào overall_impact_warning và cân nhắc tăng importance_weight cho các
  rule liên quan — đây là THÔNG TIN NGỮ CẢNH, không dùng trực tiếp để viết failed_sql_expr (đây là so sánh
  giữa các lần chạy, không phải điều kiện lọc từng dòng).
- add_rules(rules): thêm NHIỀU rule trong MỘT lần gọi. HÃY DÙNG TOOL NÀY. Mỗi lần gọi gom trọn
  rule của MỘT checkpoint. Số vòng lặp bị giới hạn, gọi từng rule một sẽ KHÔNG BAO GIỜ kết thúc kịp
  (đã đo: 9 checkpoint + add_rule từng cái = thất bại 4/4 lần).
- add_rule(stage, rule_id, column, description, failed_sql_expr, importance_weight, kind, observed_expr, expected): thêm 1 rule -
  chỉ dùng khi cần bổ sung lẻ một rule sau self-check — gọi tool này NHIỀU LẦN, mỗi lần đúng 1 rule (KHÔNG gộp nhiều rule vào 1 lần gọi).
- verify_accumulated_rules(): SELF-CHECK #1 — kiểm tra chất lượng CÚ PHÁP/AN TOÀN toàn bộ rule đã add_rule tới giờ.
- check_rules_against_schema(): SELF-CHECK #2 (SCHEMA-GROUNDING) — đối chiếu rule với schema THẬT của bảng,
  phát hiện rule tham chiếu cột không tồn tại. NÊN gọi trước save (save cũng tự chặn nếu cột khai báo sai).
- dry_run_rules(): SELF-CHECK #3 (nếu khả dụng cho pipeline này) — chạy thử SQL trên dữ liệu thật để bắt lỗi
  kiểu/hàm. Với pipeline hiện tại (Iceberg/Spark) tool này chưa được triển khai và sẽ báo "not_available".
- clear_accumulated_rules(): xoá SẠCH danh sách đang xây dựng. BIỆN PHÁP CUỐI CÙNG, và tool này
  TỪ CHỐI khi bạn đang giữ từ 30 rule trở lên. Rule bị từ chối hay self-check báo lỗi thì SỬA
  BẰNG add_rules: gọi lại đúng (stage, rule_id) bị nêu tên với bản đã sửa, nó GHI ĐÈ bản cũ.
  Đã đo thật: một lượt tích được 116 rule, bị từ chối vì 5 rule thiếu observed_unit, xoá sạch
  rồi hết lượt với 0 rule.
- submit_rules_for_verification(): NỘP toàn bộ rule cho khâu thẩm định — 1 trong 2 tool KẾT THÚC.
  Rule CHƯA được ghi xuống kho ở bước này; một agent khác sẽ thẩm định, đạt thì mới lưu.
- skip_generation(reason): quyết định KHÔNG sinh rule nào trong lần chạy này — tool KẾT THÚC còn lại.

Bạn BẮT BUỘC phải kết thúc bằng đúng một trong hai: submit_rules_for_verification HOẶC skip_generation.

QUAN TRỌNG VỀ PHẠM VI: bạn sinh rule cho TẤT CẢ các checkpoint trong MỘT lượt chạy này. Hãy đi lần
lượt từng checkpoint ở mục 0, mỗi checkpoint một lần gọi add_rules. Đừng dừng lại sau vài checkpoint.
Bạn BẮT BUỘC phải gọi verify_accumulated_rules VÀ check_rules_against_schema (self-check) ít nhất
một lần SAU KHI thêm rule xong và TRƯỚC KHI gọi submit_rules_for_verification — đây không phải gợi ý, mà là
yêu cầu bắt buộc của quy trình. Nếu self-check hoặc submit báo lỗi, hãy gọi add_rules để BỔ
SUNG hoặc SỬA đúng những rule bị nêu tên — add_rules GHI ĐÈ theo (stage, rule_id), nên gửi lại
đúng những rule đó với bản đã sửa là đủ, các rule còn lại giữ nguyên — rồi submit lại. TUYỆT ĐỐI
không clear_accumulated_rules để làm lại từ đầu — số vòng lặp có hạn, và xoá hết nghĩa là vứt bỏ
toàn bộ công đã làm.
Mỗi rule thêm qua add_rule/add_rules PHẢI có đủ: stage, rule_id, column, description,
failed_sql_expr, importance_weight — và thêm kind + observed_expr + expected nếu là rule mức
tập. Đúng theo OUTPUT_INVARIANTS đã mô tả ở trên.
"""

TOOLS_SCHEMA = [
    {"type": "function", "function": {
        "name": "count_active_rules",
        "description": "Đếm số rule đang active từ lần sinh rule trước (đọc từ state cục bộ).",
        "parameters": {"type": "object", "properties": {}, "required": []},
    }},
    {"type": "function", "function": {
        "name": "check_lineage_changed",
        "description": "Kiểm tra lineage cột (nguồn gốc + phép biến đổi) có thay đổi so với lần sinh rule gần nhất không.",
        "parameters": {"type": "object", "properties": {}, "required": []},
    }},
    {"type": "function", "function": {
        "name": "check_volume_anomaly",
        "description": "Kiem tra lich su row-count qua cac lan chay gan nhat, doc tu report cua chinh framework (/dq/reports), KHONG can query Spark/Iceberg va KHONG phu thuoc Marquez. Ket qua co ca row-count TUNG CHECKPOINT. So sanh GIUA CAC LAN CHAY thi khong viet thanh rule duoc - rule chi thay du lieu cua lan chay hien tai. Nhung dung no de CHON NGUONG cho rule kind=dataset trong mot lan chay (vi du moi lan deu 480-500 dong thi count(*) > 100000 la nguong co can cu), va de can nhac importance_weight.",
        "parameters": {"type": "object", "properties": {}, "required": []},
    }},
    {"type": "function", "function": {
        "name": "add_rule",
        "description": "Thêm 1 rule vào danh sách đang xây dựng. Gọi tool này NHIỀU LẦN, mỗi lần đúng 1 rule.",
        "parameters": {
            "type": "object",
            "properties": {
                "stage": {"type": "string", "description": "Checkpoint sẽ đo rule này. PHẢI là một tên trong mục 0 của context (danh sách checkpoint). Cùng một cột có thể cần rule KHÁC NHAU ở các checkpoint khác nhau."},
                "rule_id": {"type": "string", "description": "Mã rule, VD: VOLTE_COUNT_NEGATIVE."},
                "column": {"type": "string", "description": "Tên cột đầu vào bị kiểm tra."},
                "description": {"type": "string", "description": "Mô tả logic lỗi."},
                "failed_sql_expr": {"type": "string", "description": "Bieu thuc SQL inverted logic, tuong thich Spark SQL 3.x. kind=row thi PHAI bay NULL va cam aggregate; kind=dataset thi CHINH no la aggregate tra boolean."},
                "importance_weight": {"type": "integer", "description": "Trọng số 1-5."},
                "kind": {"type": "string", "enum": ["row", "dataset"], "description": "row = dem dong vi pham (mac dinh). dataset = bieu thuc aggregate tra boolean."},
                "observed_expr": {"type": "string", "description": "CHI cho kind=dataset: bieu thuc aggregate tra ve CON SO that, vi du MAX(cnt)."},
                "expected": {"type": "string", "description": "CHI cho kind=dataset: nguong ky vong doc duoc, vi du '<= 1'."},
                "observed_unit": {"type": "string", "enum": ["rows", "percent", "value"], "description": "CHI cho kind=dataset: don vi cua observed_expr. BAT BUOC voi kind=dataset - thieu la bi tu choi luu."},
            },
            "required": ["stage", "rule_id", "column", "description", "failed_sql_expr", "importance_weight"],
        },
    }},
    {"type": "function", "function": {
        "name": "add_rules",
        "description": "Them NHIEU rule trong MOT lan goi. DUNG TOOL NAY, khong dung add_rule tung cai: so vong lap bi gioi han, moi stage nen gom thanh mot lan goi. Rule trung (stage, rule_id) se GHI DE ban cu - day chinh la cach SUA mot rule bi tu choi, khong can va khong duoc xoa gi.",
        "parameters": {
            "type": "object",
            "properties": {
                "rules": {
                    "type": "array",
                    "description": "Danh sach rule. Nen gom tat ca rule cua CUNG mot stage vao mot lan goi.",
                    "items": {
                        "type": "object",
                        "properties": {
                            "stage": {"type": "string", "description": "Checkpoint se do rule nay - phai co trong muc 0 cua context."},
                            "rule_id": {"type": "string"},
                            "column": {"type": "string"},
                            "description": {"type": "string"},
                            "failed_sql_expr": {"type": "string", "description": "Bieu thuc SQL inverted logic. kind=row thi PHAI bay NULL va cam aggregate; kind=dataset thi CHINH no la aggregate tra boolean, vi du count(DISTINCT call_id) <> count(call_id)."},
                            "importance_weight": {"type": "integer", "description": "1-5"},
                            "kind": {"type": "string", "enum": ["row", "dataset"], "description": "row = dem dong vi pham (mac dinh). dataset = bieu thuc aggregate tra boolean, vi du MAX(cnt) > 1."},
                            "observed_expr": {"type": "string", "description": "CHI cho kind=dataset: bieu thuc aggregate tra ve CON SO that, vi du MAX(cnt). Khong co thi report chi biet dung/sai ma khong biet hong bao nhieu."},
                            "expected": {"type": "string", "description": "CHI cho kind=dataset: nguong ky vong doc duoc, vi du '<= 1'."},
                            "observed_unit": {"type": "string", "enum": ["rows", "percent", "value"], "description": "CHI cho kind=dataset: don vi cua observed_expr. rows = so DONG vi pham (vd count(k)-count(DISTINCT k)) -> quy doi ra ty le duoc. percent = da la 0-100 san. value = don vi khac (avg, max) -> khong quy doi duoc, diem se coi la 100%. Khai dung de diem phan anh muc do hong that."},
                        },
                        "required": ["stage", "rule_id", "column", "description", "failed_sql_expr", "importance_weight"],
                    },
                }
            },
            "required": ["rules"],
        },
    }},
    {"type": "function", "function": {
        "name": "verify_accumulated_rules",
        "description": "SELF-CHECK #1: kiem dinh deterministic toan bo rule da them (du truong, can bang ngoac, bay NULL voi kind=row, co observed_expr voi kind=dataset, khong SQL injection, importance_weight 1-5, khong trung rule_id).",
        "parameters": {"type": "object", "properties": {}, "required": []},
    }},
    {"type": "function", "function": {
        "name": "check_rules_against_schema",
        "description": "SELF-CHECK #2 (SCHEMA-GROUNDING): đối chiếu rule đã add_rule với schema THẬT của bảng đích (từ Marquez). Phát hiện rule tham chiếu cột KHÔNG tồn tại.",
        "parameters": {"type": "object", "properties": {}, "required": []},
    }},
    {"type": "function", "function": {
        "name": "dry_run_rules",
        "description": "SELF-CHECK #3: chạy thử failed_sql_expr trên dữ liệu thật. KHÔNG khả dụng cho pipeline Iceberg/Spark hiện tại (sẽ trả về not_available).",
        "parameters": {"type": "object", "properties": {}, "required": []},
    }},
    {"type": "function", "function": {
        "name": "clear_accumulated_rules",
        "description": "BIEN PHAP CUOI CUNG: xoa sach danh sach rule dang xay dung. Tool nay TU CHOI khi da tich duoc tu 30 rule tro len. Bi tu choi hay self-check bao loi thi SUA bang add_rules: goi lai voi dung (stage, rule_id) bi neu ten, ban moi se ghi de ban cu.",
        "parameters": {"type": "object", "properties": {}, "required": []},
    }},
    {"type": "function", "function": {
        "name": "submit_rules_for_verification",
        "description": "NỘP toàn bộ rule đã thêm cho khâu thẩm định. KHÔNG ghi xuống kho - rule chỉ được lưu sau khi một agent khác thẩm định đạt. Tool KẾT THÚC.",
        "parameters": {"type": "object", "properties": {}, "required": []},
    }},
    {"type": "function", "function": {
        "name": "skip_generation",
        "description": "Quyết định KHÔNG sinh rule mới trong lần chạy này. Tool KẾT THÚC.",
        "parameters": {
            "type": "object",
            "properties": {"reason": {"type": "string", "description": "Lý do quyết định bỏ qua."}},
            "required": ["reason"],
        },
    }},
]


def _rule_fields_from_schema():
    """Cac truong mot rule duoc phep mang, lay TU CHINH schema cua tool add_rules.

    KHONG chep tay danh sach nay. Da hong hai lan theo dung mot kieu: schema khai them mot
    truong, ham copy trong _tool_add_rules khong duoc sua theo, va truong do bi vut am tham.
    Lan gan nhat la `observed_unit` - cong tham dinh BAT BUOC no voi rule kind=dataset, agent
    gui dung, add_rules vut di, tham dinh tu choi, agent gui lai, vut tiep. Vong lap do da
    an tron mot luot chay 116 rule.
    """
    for t in TOOLS_SCHEMA:
        fn = t.get("function") or {}
        if fn.get("name") != "add_rules":
            continue
        item = fn["parameters"]["properties"]["rules"]["items"]
        required = list(item.get("required") or [])
        optional = [k for k in item["properties"] if k not in required]
        return required, optional
    raise RuntimeError("Khong tim thay schema cua add_rules trong TOOLS_SCHEMA")


#: (bat buoc, tuy chon) - nguon duy nhat la schema o tren.
RULE_REQUIRED_FIELDS, RULE_OPTIONAL_FIELDS = _rule_fields_from_schema()

SYSTEM_PROMPT_FALLBACK = """Bạn là AI Data Quality Gatekeeper, đóng vai một tổ hợp Multi-Agent System chuyên về Data Quality.
Đọc Column Lineage để đưa ra rule ứng với từng phép biến đổi, đọc Business Metadata để suy luận
constraints hợp lý, và dịch mỗi rule sang failed_sql_expr dạng inverted logic bắt được NULL,
tương thích Spark SQL 3.x."""


def build_system_prompt() -> str:
    skill_docs = load_skill_docs(["dq-rule-generator", "dq-sql-generator"])
    core = skill_docs if skill_docs else SYSTEM_PROMPT_FALLBACK
    return f"{core}\n{OUTPUT_INVARIANTS}{AGENT_ORCHESTRATION_INSTRUCTIONS}"


#: Kieu du lieu khi manifest chua ghi schema - khong bia ra mot kieu cu the.
UNKNOWN_TYPE = "?"
#: Duoi nguong nay coi la "chia lo qua nho" va nhac agent gom lai.
SMALL_BATCH_THRESHOLD = _env.SMALL_BATCH_THRESHOLD

#: Don vi cua observed_expr - phai trung DqRule.UNIT_*. Quyet dinh cach quy doi rule
#: muc tap ra ty le vi pham, nen khai sai la diem sai.
OBSERVED_UNITS = ("rows", "percent", "value")
#: So ten checkpoint con trong duoc liet ke trong loi nhac.
UNCOVERED_STAGES_SHOWN = _env.UNCOVERED_STAGES_SHOWN
#: Tran do dai bieu thuc lineage cua mot cot khi gui cho LLM.
LINEAGE_EXPR_MAX_CHARS = _env.LINEAGE_EXPR_MAX_CHARS
#: Tu nguong nay tro len thi clear_accumulated_rules tu choi xoa.
CLEAR_REFUSE_THRESHOLD = _env.CLEAR_REFUSE_THRESHOLD

STAGE_SECTION_HEADER = (
    "Moi cot ghi dang ten:kieu. DUNG ham chuoi (TRIM/RLIKE/LIKE/LENGTH) len cot kieu so"
    " - rule do khong bao gio kich hoat.\n"
    "Moi stage chi liet ke phan KHAC so voi stage CHA cua no (cha duoc ghi trong ngoac vuong,"
    " suy tu do trung cot chu khong phai stage lien truoc). Cot khong nhac lai nghia la van"
    " con nguyen tu cha.\n"
    "DAT RULE O CHECKPOINT SOM NHAT ma cot da ton tai: dong 'XUAT HIEN LAN DAU o day' cho"
    " biet dung diem do. Cot ghi 'quay lai tu stage khac' thi da ton tai tu truoc - dat rule"
    " o stage goc, dung dat lai o day."
)


def _build_required_coverage_section(business_metadata, dataset_name) -> str:
    """Liet ke truoc nhung cot BAT BUOC co rule, de agent khong phai phat hien bang cach bi tu choi.

    Cung mot nguon su that voi _required_coverage - hai cho doc cung mot du lieu nen khong the
    lech nhau. Neu chi dat chot o khau nop ma khong noi truoc, agent se tieu het vong lap vao
    viec doan xem con thieu gi.
    """
    documented = [c.get("name") for c in (business_metadata or {}).get("columns", [])
                  if c.get("name")]
    try:
        impacted = sorted(_impacted_columns(dataset_name))
    except Exception:
        impacted = []
    extra_impacted = [c for c in impacted if c not in documented]

    lines = [
        "submit_rules_for_verification se TU CHOI neu con cot nao duoi day chua co rule.",
        "",
        f"A. {len(documented)} cot duoc mo ta trong business metadata (muc 3) - dua duoc mot cot",
        "   vao file metadata da la tuyen bo rang no quan trong:",
        f"   {', '.join(documented) if documented else '(khong co)'}",
    ]
    if extra_impacted:
        lines += [
            "",
            f"B. {len(extra_impacted)} cot LINEAGE ghi nhan CO BIEN DOI (khong phai IDENTITY) ma",
            "   muc A chua nhac toi. Cot bi bien doi co the hong do chinh pipeline:",
            f"   {', '.join(extra_impacted)}",
        ]
    lines += [
        "",
        "Dat rule o checkpoint SOM NHAT ma cot da ton tai (xem muc 0) - bat loi o nguon re hon",
        "va chi dung thu pham hon. Cot khong nam trong danh sach tren thi tuy y.",
    ]
    return "\n".join(lines)


def _build_stage_section(dq_key: str) -> str:
    """Muc 0 cua context: pipeline co nhung checkpoint nao va cot nao co mat o tung diem."""
    stage_cols, order = load_stage_columns(dq_key)
    if not order:
        return ("Pipeline nay CHUA khai bao checkpoint nao (chua co manifest). Hay dat stage="
                f"\"{DEFAULT_STAGE}\" cho MOI rule - do la diem ngay truoc lenh ghi.")
    # PHAI co KIEU DU LIEU, khong chi ten cot. Da do that 21/09/2026: hai lan chay lien tiep
    # deu bi Verify tu choi vi Gen Rules viet TRIM() len cot kieu int (call_id, infra_ci).
    # Agent khong doan bua - no MU: muc 0 truoc day chi liet ke ten cot, trong khi manifest da
    # mang san schema[].type. Rule TRIM(call_id) tren cot int khong bao gio kich hoat, nen stage
    # do bao 1.0 trong khi chang kiem gi ca.
    #
    # Ma hoa DELTA theo cay cha-con thay vi liet ke day du tung stage. Do that tren pipeline
    # nay: liet ke day du 574 dong, delta theo thu tu tuyen tinh 265 dong (46%), delta theo cay
    # cha-con 112 dong (20%). Ngoai chuyen tiet kiem, ban delta doc ra dung hinh pipeline - cai
    # ma ban liet ke day du khong the hien duoc.
    def column_types(stage):
        schema = stage.get("schema") or []
        if schema:
            return {c.get("name"): c.get("type", UNKNOWN_TYPE) for c in schema}
        return {c: UNKNOWN_TYPE for c in (stage.get("columns") or [])}

    def format_columns(types_by_name, names):
        return ", ".join(f"{n}:{types_by_name.get(n, UNKNOWN_TYPE)}" for n in names)

    # Suy CHA bang do trung cot, khong lay stage lien truoc theo thu tu.
    #
    # Da tra gia cho cho nay 21/09/2026: diff tuyen tinh khien tf_decode bi doi chieu voi
    # src_infra - mot NHANH NGUON KHAC HAN, 3 cot - nen 74 cot cua src_calls bi in lai duoi
    # nhan "THEM so voi stage truoc". Agent doc va ket luan hop ly rang chung xuat hien lan dau
    # o tf_decode, roi dat 61/88 rule vao do thay vi o src_calls. Do phu cac stage con lai tut
    # xuong 1/81. Bieu dien sai du lieu thi agent suy luan sai, va no khong co cach nao biet.
    lines = []
    seen_before = {}                      # ten cot -> stage dau tien nhin thay
    history = []                          # (ten stage, dict kieu) theo thu tu da duyet

    for stage in order:
        name = stage.get("stage")
        types_by_name = column_types(stage)
        columns = list(types_by_name)
        duplicates = stage.get("duplicate_names") or []

        # Cha SUY ra tu do trung cot, khong bat dev khai. Da kiem tren pipeline nay: tf_decode
        # duoc suy dung ve src_calls (trung 74 cot) chu khong phai src_infra dung lien truoc no
        # (trung 0 cot). Mot tham so khong bat buoc la mot tham so co the khai sai, va thu tu
        # goi dq.check() da du de dung lai cay.
        parent_name, parent_types, best_overlap = None, {}, 0
        for earlier_name, earlier_types in history:
            overlap = len(set(columns) & set(earlier_types))
            if overlap >= best_overlap and overlap > 0:
                parent_name, parent_types, best_overlap = earlier_name, earlier_types, overlap

        header = (f"- stage=\"{name}\" (thu tu {stage.get('order')}, "
                  f"{stage.get('row_count', 'chua do')} dong o lan chay truoc, "
                  f"{len(columns)} cot)")
        body = []
        if parent_name is None:
            header += "   [nhanh nguon rieng]"
            body.append(f"    cot: {format_columns(types_by_name, columns)}")
        else:
            header += f"   [cha = {parent_name}]"
            added = [c for c in columns if c not in parent_types]
            removed = [c for c in parent_types if c not in types_by_name]
            first_here = [c for c in added if c not in seen_before]
            again = [(c, seen_before[c]) for c in added if c in seen_before]
            if first_here:
                body.append("    XUAT HIEN LAN DAU o day: "
                            + format_columns(types_by_name, first_here))
            if again:
                body.append("    quay lai tu stage khac (DA ton tai tu truoc, dung coi la moi): "
                            + ", ".join(f"{c} (co tu {src})" for c, src in again))
            if removed:
                body.append(f"    BO di so voi cha: {', '.join(removed)}")
            if not body:
                body.append("    (khong doi so voi cha)")

        lines.append(header + "\n" + "\n".join(body))
        if duplicates:
            lines.append(f"    CANH BAO cot TRUNG TEN o stage nay: {', '.join(duplicates)}"
                         " - rule tham chieu cac cot nay se AMBIGUOUS_REFERENCE,"
                         " dung dat rule len chung.")
        for column in columns:
            seen_before.setdefault(column, name)
        history.append((name, types_by_name))

    return (STAGE_SECTION_HEADER + "\n\n" + "\n".join(lines)
            + "\n\n" + _stage_lookup_table(seen_before))


def _stage_lookup_table(first_stage_of: dict) -> str:
    """Bang tra cuu NGUOC: cot -> checkpoint som nhat cot do ton tai.

    Ban delta o tren da mang du thong tin nay, nhung RAI theo thu tu pipeline: muon biet
    `prb_growth_per_day` song o dau thi phai doc het 8 khoi roi tu dung lai cay cha-con.
    Agent dung sai 6 lan lien tiep 22/09/2026 va deu sai MOT HUONG - dat cot som hon noi no
    ra doi: prb_growth_per_day va days_to_congestion (tf_capacity) bi dat o agg_daily,
    dq_score (tf_standardize) bi dat o src_raw. Ca ba luot regen chay het vi cung mot loi.

    Sap xep theo VAN CHU CAI de tra cuu duoc mot buoc. Va liet ke DU moi cot, vi danh sach
    day du la thu duy nhat bac bo duoc cot bia: luot do agent viet rule cho `max_ul_prb`,
    mot cot khong ton tai o bat ky stage nao (co avg_ul_prb, p95_ul_prb, max_dl_prb).
    """
    if not first_stage_of:
        return ""
    items = [f"{col}={stage}" for col, stage in sorted(first_stage_of.items())]
    lines, cur = [], []
    for item in items:
        cur.append(item)
        if len(cur) == 4:
            lines.append("  " + "  ".join(cur))
            cur = []
    if cur:
        lines.append("  " + "  ".join(cur))
    return ("TRA CUU cot -> checkpoint SOM NHAT duoc phep dat rule (dat o stage sau do cung"
            " hop le, dat TRUOC do thi cot chua ton tai va rule bi tu choi).\n"
            "Cot KHONG co trong bang nay thi KHONG ton tai o bat ky checkpoint nao"
            " - dung viet rule cho no.\n" + "\n".join(lines))


def _trim_lineage_for_llm(lineage: dict) -> dict:
    """Cat bieu thuc `transformation` qua dai TRUOC khi gui cho LLM.

    `derived_from` giu nguyen - do la thong tin cau truc, nho, va la thu agent thuc su can
    de biet cot nay bat nguon tu dau. Chi cat phan van ban bieu thuc: no do OpenLineage trai
    phang tu Catalyst nen ti le voi DO PHUC TAP phep bien doi chu khong voi so cot.

    Noi RO da cat, khong cat im lang: agent doc mot bieu thuc cut ma tuong la day du se suy
    ra mien gia tri sai.
    """
    out = {}
    for name, value in lineage.items():
        if not isinstance(value, dict):
            out[name] = value
            continue
        expr = value.get("transformation")
        if isinstance(expr, str) and len(expr) > LINEAGE_EXPR_MAX_CHARS:
            value = dict(value)
            value["transformation"] = (
                expr[:LINEAGE_EXPR_MAX_CHARS]
                + f"  ...[DA CAT {len(expr) - LINEAGE_EXPR_MAX_CHARS} ky tu."
                  " Bieu thuc day du qua dai de gui. Dung `derived_from` de biet cot nguon,"
                  " va dung doan tren de biet KIEU phep bien doi. KHONG suy mien gia tri tu"
                  " mot bieu thuc da cat - neu can chinh xac thi hoi qua query_live_stage.]")
            value["transformation_truncated"] = True
        out[name] = value
    return out


def build_context_prompt(dataset_name: str, yaml_path: str = None, dq_key: str = None) -> str:
    lineage_context = dict(get_column_lineage(dataset_name))
    indirect = lineage_context.pop("__indirect_impact__", None)
    lineage_context = _trim_lineage_for_llm(lineage_context)

    downstream_datasets = get_downstream_datasets(dataset_name)

    business_metadata, _source = pipeline_config.load_business_metadata(yaml_path)

    if not lineage_context:
        lineage_note = "Chưa có Column Lineage. Hãy BỎ QUA nhóm Predictive Error Rules và CHỈ sinh Business Rules từ YAML bên dưới."
    else:
        lineage_note = json.dumps(lineage_context, indent=2, ensure_ascii=False)

    indirect_note = (
        json.dumps(indirect, indent=2, ensure_ascii=False)
        if indirect else "Không có phụ thuộc INDIRECT (JOIN/FILTER/GROUP_BY) nào được ghi nhận."
    )
    downstream_note = ", ".join(downstream_datasets) if downstream_datasets else "Không có bảng hạ nguồn nào được ghi nhận trên Marquez."

    stage_note = _build_stage_section(dq_key or _dq_key(dataset_name))
    required_note = _build_required_coverage_section(business_metadata, dataset_name)

    return f"""=== 0. CÁC CHECKPOINT CỦA PIPELINE (nơi rule sẽ được ĐO) ===
Dev đã đặt sẵn các điểm đo dưới đây. Mỗi rule PHẢI khai báo stage là MỘT trong các tên này.

{stage_note}

QUY TẮC ĐẶT STAGE - đọc kỹ, đây là phần dễ sai nhất:
* Đặt rule ở checkpoint SỚM NHẤT mà cột đó đã tồn tại và phép kiểm đã có nghĩa. Bắt lỗi ở
  nguồn rẻ hơn và chỉ đúng thủ phạm hơn là bắt ở bảng đầu ra.
* Cùng một cột CÓ THỂ cần rule khác nhau ở hai checkpoint, và đó là chủ đích chứ không phải
  trùng lặp - vì phép biến đổi ở giữa làm đổi cả giá trị lẫn số dòng. Ví dụ thật của pipeline
  này: sau coalesce(city_code,'unknown') thì end_province KHÔNG BAO GIỜ NULL nữa, nên rule
  "IS NULL" đặt sau đó là rule CHẾT; muốn bắt join trượt thì phải kiểm end_province='unknown'
  ở checkpoint sau join, hoặc kiểm city_code IS NULL ở checkpoint trước đó.
* TRƯỚC KHI viết rule, hãy đối chiếu cột với danh sách cột của đúng stage ở trên. Cột chỉ có ở
  bảng nguồn (ví dụ payload thô, các cột _new) chỉ kiểm được ở checkpoint nguồn.
* Điểm chất lượng tính RIÊNG cho từng checkpoint, không gộp - nên đừng ngại đặt nhiều rule ở
  một checkpoint và ít ở checkpoint khác.

CỘT CÓ ĐIỀU KIỆN - NULL không phải lúc nào cũng là lỗi:
* Cột sinh bởi `when(<điều kiện>, ...).otherwise(null)` thì NULL là giá trị ĐÚNG cho mọi dòng
  không thoả điều kiện, và thường đó là ĐA SỐ dòng. Viết "col IS NULL" lên cột như vậy là báo
  hỏng gần như toàn bộ bảng.
* Nhận ra bằng HAI nguồn, không phải bằng linh cảm: `kinds` chứa CONDITIONAL ở mục 1, hoặc
  trường `no_value_when` của cột ở mục 2 — trường này nói thẳng cột vắng giá trị trong tình
  huống nào. Cột có `no_value_when` thì `<cột> IS NULL` trần bị add_rules TỪ CHỐI.
* Khi đó có hai cách viết đúng, KHÔNG được viết "col IS NULL" trần:
    - kiểm miền giá trị và bỏ qua NULL:  col IS NOT NULL AND <ngoài miền>
    - buộc có giá trị theo điều kiện:    <cột cờ> AND col IS NULL
* Cột KHÔNG có trong mục 1 (lineage) và KHÔNG có mô tả nghiệp vụ ở mục 2 thì bạn chỉ biết tên
  và kiểu. Đừng suy ra rằng nó bắt buộc NOT NULL - đó là phỏng đoán. Hãy kiểm miền giá trị
  bằng dạng "IS NOT NULL AND ..." và để phần NULL cho lượt sau, khi đã có thêm thông tin.
* Mục 2 là nơi DUY NHẤT nói được nghĩa của một cột: đơn vị, miền giá trị, mã số, và khi nào
  cột không có giá trị. Tên cột KHÔNG nói được những điều đó. Một cột mã số có thể đếm từ 0
  hoặc từ 1, và đoán sai một đầu là báo hỏng phần lớn bảng.

=== 1. CẤU TRÚC DỮ LIỆU & NGUỒN GỐC (COLUMN LINEAGE - DIRECT, theo từng cột đầu ra) ===
{lineage_note}

Mỗi cột kèm `kinds` — LOẠI phép biến đổi sinh ra nó, lấy từ chính lineage chứ không phải
suy đoán. ĐỌC NÓ TRƯỚC KHI VIẾT RULE CHO CỘT ĐÓ:

  IDENTITY        Sao chép nguyên từ nguồn. Rule thuộc về checkpoint SỚM NHẤT cột này
                  xuất hiện, không phải ở đây.
  TRANSFORMATION  Biến đổi vô hướng (substring, cast, round). Miền giá trị của cột NGUỒN
                  KHÔNG còn đúng: substring(imsi,1,8) không còn 15 ký tự.
  AGGREGATION     Sinh từ phép gom — HẠT DỮ LIỆU ĐỔI TẠI ĐÂY. Rule uniqueness trên khoá
                  nhóm chỉ đúng từ checkpoint này TRỞ ĐI, KHÔNG đúng ở bất kỳ chặng nào
                  trước nó. Bất biến mức dòng của nguồn cũng không còn áp được.
  CONDITIONAL     Sinh từ CASE WHEN. NULL RẤT CÓ THỂ là một nhánh hợp lệ — ĐỪNG viết
                  NOT NULL trừ khi bạn thấy rõ MỌI nhánh đều trả giá trị. Thay vào đó hãy
                  viết rule MIỀN ĐÓNG (giá trị phải thuộc tập các nhánh): vừa an toàn hơn
                  vừa bắt được nhiều lỗi hơn.
  WINDOW          Hàm cửa sổ. NULL ở đầu mỗi phân hoạch là ĐÚNG THIẾT KẾ (lag, cửa sổ
                  trượt cần đủ mẫu). ĐỪNG viết NOT NULL.

Đã trả giá thật 22/09/2026: năm rule NOT NULL trên các cột CONDITIONAL của một pipeline vi
phạm 94.2% trên dữ liệu hoàn toàn đúng, vì NULL ở đó là nhánh hợp lệ của CASE WHEN.

=== 1b. PHỤ THUỘC GIÁN TIẾP (INDIRECT - JOIN/FILTER/GROUP_BY, ảnh hưởng toàn bộ dòng) ===
{indirect_note}

`__group_by_columns__` liệt kê các cột từng xuất hiện trong MỘT GROUP_BY nào đó của
pipeline. Đây là KHOÁ ỨNG VIÊN, KHÔNG phải hạt dữ liệu xác định — danh sách gộp nhiều phép
GROUP_BY ở nhiều bước khác nhau. Tín hiệu thật của nó là: HẠT DỮ LIỆU ĐỔI GIỮA CÁC CHẶNG.

Trước khi viết BẤT KỲ rule uniqueness nào, xác định hạt của CHÍNH checkpoint đó. Đừng mượn
hạt của bảng đầu ra: một phát biểu kiểu "(A, B) là khoá duy nhất" trong business metadata
gần như luôn nói về bảng ĐẦU RA, và áp nó ở chặng trước AGGREGATION là sai chắc chắn.
Đã trả giá thật: rule `count(DISTINCT (event_date, cell_name)) <> count(*)` đặt ở một chặng
dữ liệu THEO GIỜ, vi phạm 95.75% trên dữ liệu đúng.

=== 2. MỤC ĐÍCH SỬ DỤNG DỮ LIỆU (DOWNSTREAM DATASETS) ===
{downstream_note}

=== 3. TRI THỨC NGHIỆP VỤ (BUSINESS METADATA) ===
{yaml.dump(business_metadata, allow_unicode=True, default_flow_style=False)}

=== 4. ĐỘ PHỦ BẮT BUỘC (kiểm bằng code, không phải lời khuyên) ===
{required_note}
"""


def _normalize_lineage(lineage_context: dict) -> str:
    """Dang chuan de bam hash - phai ON DINH giua hai lan chay cung mot job.

    Moi khoa bat dau bang `__` la SIEU DU LIEU cua ca dataset, khong phai mot cot, va gia tri
    cua no khong phai dict. Truoc day cho nay dat cach theo DUNG MOT TEN (`__indirect_impact__`)
    nen them mot khoa sieu du lieu thu hai la no roi vao nhanh cot va no `.get()` tren mot
    list. Da xay ra that 22/09/2026 khi them `__group_by_columns__`: submit_rules_for_verification
    chet voi "'list' object has no attribute 'get'" sau 41 lan add_rules - mat tron mot luot
    sinh rule. Dung quy uoc tien to `__` nhu _impacted_columns van lam.
    """
    normalized = {}
    for col_name, col_data in lineage_context.items():
        if col_name.startswith("__"):
            if isinstance(col_data, list):
                normalized[col_name] = sorted(
                    f"{d['source']}:{d['subtype']}" if isinstance(d, dict) else str(d)
                    for d in col_data)
            else:
                normalized[col_name] = col_data
            continue
        normalized[col_name] = {
            "derived_from": sorted(col_data.get("derived_from", [])),
            "transformation": col_data.get("transformation", "IDENTITY"),
            # `kinds` vao hash: doi loai phep bien doi la doi ban chat cot, phai sinh lai rule.
            "kinds": sorted(col_data.get("kinds") or []),
        }
    return json.dumps(normalized, sort_keys=True, ensure_ascii=False)


def _compute_lineage_hash(dataset_name: str) -> str:
    lineage_context = get_column_lineage(dataset_name)
    return hashlib.sha256(_normalize_lineage(lineage_context).encode("utf-8")).hexdigest()


# ---- Deterministic self-check (verify) ----

def tool_verify_rules(rules: list):
    issues = []
    if not rules:
        return False, ["Danh sách rules rỗng."]

    seen_ids = set()
    for i, rule in enumerate(rules):
        tag = f"rule[{i}] (stage={rule.get('stage', '?')}, rule_id={rule.get('rule_id', '?')})"
        for field in ("stage", "rule_id", "column", "description", "failed_sql_expr"):
            if not str(rule.get(field, "")).strip():
                issues.append(f"{tag}: thiếu hoặc rỗng trường bắt buộc '{field}'.")

        expr = str(rule.get("failed_sql_expr", "")).strip()
        if expr:
            if expr.count("(") != expr.count(")"):
                issues.append(f"{tag}: dấu ngoặc không cân bằng trong failed_sql_expr.")
            # Rule MUC TAP la mot aggregate tra boolean (vi du count(DISTINCT k) <> count(k)),
            # no khong lam viec voi tung dong nen khong co NULL de bay. Ap yeu cau IS NULL vao
            # day la ep agent viet mot bieu thuc vo nghia.
            if rule.get("kind") != "dataset" and not mentions_null(expr):
                issues.append(f"{tag}: failed_sql_expr" + NULL_NOTE)
            if rule.get("kind") == "dataset" and not str(rule.get("observed_expr", "")).strip():
                issues.append(f"{tag}: rule kind=dataset PHẢI có 'observed_expr' — boolean chỉ "
                              f"cho biết hỏng hay không, không cho biết hỏng bao nhiêu.")
            if rule.get("kind") == "dataset" and rule.get("observed_unit") not in OBSERVED_UNITS:
                issues.append(f"{tag}: rule kind=dataset PHẢI khai 'observed_unit' là một trong "
                              f"{', '.join(OBSERVED_UNITS)} — thiếu thì mọi vi phạm đều bị tính "
                              f"là hỏng 100%% dù con số thật nhỏ hơn nhiều.")
            upper_expr = f" {expr.upper()} "
            for bad in FORBIDDEN_SQL_TOKENS:
                if bad in upper_expr:
                    issues.append(f"{tag}: failed_sql_expr chứa token không hợp lệ '{bad.strip()}'.")

        try:
            weight = int(rule.get("importance_weight", 1))
            if weight < 1 or weight > 5:
                issues.append(f"{tag}: importance_weight={weight} ngoài khoảng [1..5].")
        except (ValueError, TypeError):
            issues.append(f"{tag}: importance_weight không phải số nguyên.")

        # Khoa la (stage, rule_id), khong phai rule_id: cung mot rule_id o HAI checkpoint khac
        # nhau la hop le va thuong la co y - vi du kiem cung mot cot truoc va sau mot phep bien
        # doi. Trung trong CUNG mot stage moi la loi.
        key = (rule.get("stage") or DEFAULT_STAGE, rule.get("rule_id"))
        if key in seen_ids:
            issues.append(f"{tag}: rule_id '{key[1]}' bị trùng trong cùng stage '{key[0]}'.")
        seen_ids.add(key)

    return (len(issues) == 0), issues


# ---- Schema-grounding self-check ----

_STRING_LITERAL_RE = re.compile(r"'[^']*'|\"[^\"]*\"")


NULL_NOTE = (
    # Guard nay sinh ra de chan rule CHET: `col IS NULL` dat sau coalesce thi khong bao gio
    # ban. Nhung no tung viet la "bieu thuc PHAI chua chuoi 'IS NULL'", va nhu the thi
    # `col IS NOT NULL AND col < 0` - rule dung cho cot ma NULL la hop le - bi tu choi, vi
    # "IS NOT NULL" khong chua chuoi con "IS NULL".
    #
    # Hau qua do that 22/09/2026 tren cell_capacity_daily: incident_hours, incident_seq,
    # incident_peak_prb... deu la `when(is_congested, ...).otherwise(null)`, NULL o 94.2% so
    # dong la DUNG. Guard ep agent viet `incident_hours IS NULL OR ...`, tuc la bao 94.2% dong
    # vi pham. Nam rule sai lien tiep qua nhieu luot sinh, va khong luot nao co the sua duoc
    # vi cach dien dat dung bi cam.
    #
    # Y dinh that cua guard la "agent PHAI co y kien ve NULL", khong phai "phai coi NULL la
    # hong". `IS NOT NULL` the hien y kien do ro rang khong kem: no noi NULL chap nhan duoc o
    # cot nay.
    " PHAI noi ro y dinh voi NULL: 'IS NULL' neu NULL la vi pham, hoac 'IS NOT NULL' neu NULL"
    " la hop le o cot nay (vi du cot chi co gia tri khi mot dieu kien dung)."
)


def bare_null_check(expr: str, column: str) -> bool:
    """Bieu thuc coi NULL cua `column` la vi pham, khong kem dieu kien nao.

    Phan biet ba dang:
        col IS NULL OR col < 0          -> True,  NULL bi coi la hong
        col IS NOT NULL AND col < 0     -> False, NULL duoc bo qua
        is_congested AND col IS NULL    -> False, NULL chi hong khi co bat
    Dang thu ba nhan ra bang viec co mot cot KHAC dung truoc `AND`, nen chi can kiem tra
    quanh vi tri `col IS NULL`.
    """
    up = expr.upper()
    target = f"{column.upper()} IS NULL"
    idx = up.find(target)
    if idx < 0:
        return False
    if up[max(0, idx - 4):idx + len(target)].strip().startswith("NOT "):
        return False
    before = up[:idx].rstrip()
    # Co dieu kien dan truoc bang AND thi NULL da duoc thu hep pham vi, khong con la tran.
    return not before.endswith("AND")


def mentions_null(expr: str) -> bool:
    """Bieu thuc co xu ly NULL mot cach tuong minh chua - theo huong nao cung duoc."""
    return "IS NULL" in expr.upper() or "IS NOT NULL" in expr.upper()


def _columns_referenced_in_expr(expr: str) -> set:
    out = set()
    expr = expr or ""
    # Strip quoted string literals first: a LIKE pattern such as '____-__-__' would otherwise
    # be scanned as identifiers and reported as unknown columns (false-positive soft warning).
    expr = _STRING_LITERAL_RE.sub(" ", expr)
    for m in _IDENT_RE.finditer(expr):
        tok = m.group(0)
        if expr[m.end():m.end() + 1] == "(":
            continue
        if tok.lower() in _SQL_NON_COLUMN_TOKENS:
            continue
        out.add(tok)
    return out


DEFAULT_STAGE = "final"


def load_stage_columns(dq_key: str):
    """{stage: set(cot co mat o stage do)} tu manifest lan chay truoc. {} = chua co manifest.

    Dung de schema-grounding dung dan: mot rule dat o checkpoint sau khi doc nguon phai duoc
    doi chieu voi schema CUA NGUON, khong phai schema bang dau ra - hai cai khac nhau that
    (bang nguon co location_report_payload, imsi_new...; bang dau ra co date_hour, end_lac...).
    """
    try:
        from dq_store import get_stages
        doc = get_stages(dq_key) or {}
    except Exception as e:
        print(f"Khong doc duoc manifest stage: {e}")
        return {}, []
    stages, order = {}, []
    for st in doc.get("stages", []):
        name = st.get("stage")
        if not name:
            continue
        stages[name] = set(st.get("columns") or [])
        order.append(st)
    return stages, order


def tool_schema_grounding(rules: list, schema: dict, stage_columns: dict = None):
    if not schema:
        return [], ["Không lấy được schema từ Marquez — bỏ qua schema-grounding (không chặn)."]

    output_cols = set(schema.keys())
    stage_columns = stage_columns or {}
    hard_issues, soft_warnings = [], []
    for i, rule in enumerate(rules):
        stage = str(rule.get("stage") or DEFAULT_STAGE)
        tag = f"rule[{i}] (stage={stage}, rule_id={rule.get('rule_id', '?')})"

        # Cot hop le o stage nao thi doi chieu schema cua dung stage do. Chua co manifest cho
        # stage nay thi lui ve schema bang dau ra - va chi canh bao, khong chan: lan chay dau
        # tien chua co manifest, chan o day la khong bao gio sinh duoc rule.
        known = stage in stage_columns
        cols = stage_columns[stage] if known else output_cols

        col = str(rule.get("column", "")).strip()
        if col and col not in cols:
            msg = f"{tag}: cột '{col}' KHÔNG có ở stage này. Cột hợp lệ: {sorted(cols)}."
            (hard_issues if known else soft_warnings).append(msg)
        for ref in _columns_referenced_in_expr(rule.get("failed_sql_expr", "")):
            if ref not in cols:
                soft_warnings.append(f"{tag}: định danh '{ref}' trong failed_sql_expr không khớp cột nào ở stage này.")
    return hard_issues, soft_warnings


VOLUME_ANOMALY_THRESHOLD_PCT = _env.VOLUME_ANOMALY_THRESHOLD_PCT
# "range ngày/giờ tùy pipeline": how many PRIOR DISTINCT PARTITIONS to average over as the
# baseline. Default 7 suits a daily partition column (a trailing week); an hourly-partitioned
# pipeline would want something like 24 (a trailing day) passed via env instead.
VOLUME_HISTORY_WINDOW = _env.VOLUME_HISTORY_WINDOW
# A write's newest file mtime should land at/near job end; this just absorbs clock skew and the
# gap between "last row written" and "job process exits" -- not a tuning knob for anything else.
PARTITION_MATCH_BUFFER_MS = _env.PARTITION_MATCH_BUFFER_MS
DQ_PARTITION_COLUMN = _env.PARTITION_COLUMN


def _iso_to_epoch_ms(iso_str: str):
    if not iso_str:
        return None
    try:
        return int(datetime.fromisoformat(iso_str.replace("Z", "+00:00")).timestamp() * 1000)
    except ValueError:
        return None


def _match_run_to_partition(run: dict, partitions: list) -> str:
    """Best-effort: which partition folder's newest file mtime is closest to (and within
    PARTITION_MATCH_BUFFER_MS of) this run's endedAt. Returns None if no partition matches --
    e.g. Marquez has the run but its written files were since compacted/expired.
    """
    ended_ms = _iso_to_epoch_ms(run.get("ended_at"))
    if ended_ms is None:
        return None
    best_value, best_delta = None, None
    for p in partitions:
        delta = abs(p["latest_mtime_ms"] - ended_ms)
        if delta <= PARTITION_MATCH_BUFFER_MS and (best_delta is None or delta < best_delta):
            best_value, best_delta = p["value"], delta
    return best_value


def tool_check_volume_anomaly(dataset_name: str) -> dict:
    """Compare the latest partition's row count to a trailing window of prior partitions.

    Two-step design, not a single Spark/HDFS count:
      1. lineage_client.get_row_count_history() -- exact row counts per RUN, from Marquez
         (OpenLineage's inputStatistics/outputStatistics facets, sourced from Iceberg's own
         snapshot commit -- authoritative, no Spark query needed).
      2. hdfs_client.list_partition_values() -- which PARTITION (date/hour) each run actually
         wrote to, from HDFS folder names (Marquez's run history doesn't carry the job's
         PROCESS_DATE argument, only parent-run linkage metadata). Runs are matched to
         partitions by nearest file mtime to the run's endedAt, within PARTITION_MATCH_BUFFER_MS.
      Deliberately NOT counting rows/files directly from HDFS: Iceberg's dynamic partition
      overwrite leaves superseded data files physically in place until a snapshot-expiry
      compaction runs, so a raw directory listing overcounts -- only Iceberg's own snapshot
      commit (which is what Marquez's rowCount reflects) is authoritative.

    Threshold-based (not full statistics): flags a swing beyond
    +/-DQ_VOLUME_ANOMALY_THRESHOLD_PCT% (default 30) vs the mean of the trailing
    DQ_VOLUME_HISTORY_WINDOW (default 7) prior DISTINCT partitions. This is per-partition
    history context, not a per-row predicate, so it can never become a failed_sql_expr -- it's
    context for the LLM (importance_weight, overall_impact_warning), not a rule ingredient. A
    real "row count must be within N% of the trailing average" dataset-level rule would need a
    new rule TYPE (our schema is column/row-predicate only) -- worth adding once the Execute
    Rules agent exists to actually run it.
    """
    history = get_row_count_history(dataset_name)
    if len(history) < 2:
        return {
            "status": "insufficient_history",
            "runs_found": len(history),
            "history": history,
            "note": "Cần ít nhất 2 lần chạy đã COMPLETED để so sánh — chưa đủ dữ liệu lịch sử.",
        }

    partitions = list_partition_values(f"{dataset_name}/data", DQ_PARTITION_COLUMN)
    for run in history:
        run["partition_value"] = _match_run_to_partition(run, partitions)

    # Keep only the most recent run per DISTINCT partition (a partition may have been
    # overwritten/rerun several times -- the latest run's count is the one that matters).
    latest_per_partition = {}
    for run in history:
        pv = run.get("partition_value")
        if pv is None:
            continue
        prev = latest_per_partition.get(pv)
        if prev is None or (run.get("ended_at") or "") > (prev.get("ended_at") or ""):
            latest_per_partition[pv] = run

    distinct_partitions = sorted(latest_per_partition.keys())  # zero-padded date/hour strings sort chronologically
    unmatched_runs = sum(1 for r in history if r.get("partition_value") is None)

    if len(distinct_partitions) < 2:
        return {
            "status": "insufficient_history",
            "runs_found": len(history),
            "distinct_partitions_found": distinct_partitions,
            "unmatched_runs": unmatched_runs,
            "history": history,
            "note": (
                f"Chỉ thấy {len(distinct_partitions)} partition ({DQ_PARTITION_COLUMN}) riêng biệt "
                f"trong {len(history)} lần chạy — chưa đủ để so sánh xu hướng theo thời gian "
                "(nhiều lần chạy có thể đang ghi đè CÙNG một partition)."
            ),
        }

    latest_partition = distinct_partitions[-1]
    baseline_partitions = distinct_partitions[max(0, len(distinct_partitions) - 1 - VOLUME_HISTORY_WINDOW):-1]
    latest_run = latest_per_partition[latest_partition]

    result = {
        "status": "ok",
        "latest_partition": latest_partition,
        "baseline_partitions": baseline_partitions,
        "unmatched_runs": unmatched_runs,
    }

    # The most recently EXECUTED run should normally also be the one writing the newest
    # partition. If some other run wrote a newer partition than the last-executed one, this
    # run is a backfill/rerun of an older date -- flag it distinctly, since comparing it to
    # "the trailing window before the newest partition" would be the wrong baseline.
    most_recent_run = history[-1]
    if most_recent_run.get("partition_value") not in (None, latest_partition):
        result["status"] = "backdated_write"
        result["note"] = (
            f"Lần chạy gần nhất (kết thúc {most_recent_run.get('ended_at')}) ghi vào partition "
            f"'{most_recent_run.get('partition_value')}', KHÔNG phải partition mới nhất đã biết "
            f"('{latest_partition}') — có vẻ là backfill/rerun ngày cũ, không nên so sánh volume "
            "trực tiếp với xu hướng gần đây."
        )
        return result

    for field in ("input_row_count", "output_row_count"):
        baseline_vals = [latest_per_partition[p].get(field) for p in baseline_partitions]
        baseline_vals = [v for v in baseline_vals if v]
        if not baseline_vals:
            continue
        baseline_mean = sum(baseline_vals) / len(baseline_vals)
        latest_val = latest_run.get(field)
        if latest_val is None or baseline_mean == 0:
            continue
        pct_change = (latest_val - baseline_mean) / baseline_mean * 100
        result[f"{field}_baseline_mean"] = round(baseline_mean, 1)
        result[f"{field}_pct_change"] = round(pct_change, 1)
        if abs(pct_change) >= VOLUME_ANOMALY_THRESHOLD_PCT:
            result["status"] = "anomaly"
            result.setdefault("anomalies", []).append(
                f"{field}: baseline trung bình {baseline_mean:.1f} ({len(baseline_vals)} partition "
                f"trước) -> {latest_val} tại partition '{latest_partition}' ({pct_change:+.1f}%, "
                f"vượt ngưỡng ±{VOLUME_ANOMALY_THRESHOLD_PCT:.0f}%)"
            )
    return result


def _required_coverage(rules, business_metadata, dataset_name, dq_key):
    """Tu choi nop neu cot BAT BUOC chua co rule. Tra dict loi, hoac None khi du.

    Hai nhom bat buoc, va ca hai deu do DU LIEU quyet dinh chu khong do agent tu chon:

      mo ta     moi cot liet ke trong `columns` cua business metadata. Khong co co danh dau
                rieng: dua duoc mot cot vao file metadata da la tuyen bo rang no quan trong.
                Cung quy uoc voi metadata cua sub-voice-call-daily.
      impacted  cot ma lineage cho thay CO BIEN DOI (subtype khac IDENTITY). Cot bi bien doi la
                cot co the hong do chinh pipeline, khac han cot chi di xuyen qua.

    Vi sao chot bang code chu khong dan trong prompt: da do that 21/09/2026, prompt khong he
    cam gi ve do phu nen agent tu quyet dinh dung o dau va ra 1/81 cot o jn_infra, 10/75 o
    src_calls. Verify agent co bat (cham jn_infra = FAIL) nhung verdict tong van WEAK nen bo
    rule van duoc ghi.
    """
    covered_anywhere = {r.get("column") for r in rules if r.get("column")}
    problems = []

    documented = [c.get("name") for c in (business_metadata or {}).get("columns", [])
                  if c.get("name")]
    missing_documented = sorted(n for n in documented if n not in covered_anywhere)
    if missing_documented:
        problems.append({
            "loai": "cot_co_mo_ta_chua_co_rule",
            "so_luong": len(missing_documented),
            "cot": missing_documented,
            "vi_sao": "Cac cot nay duoc mo ta trong business metadata, tuc la cot quan trong."
                      " Moi cot trong `columns` deu phai co it nhat mot rule."})

    try:
        impacted = sorted(_impacted_columns(dataset_name) - covered_anywhere)
    except Exception:
        impacted = []
    if impacted:
        problems.append({
            "loai": "cot_bi_bien_doi_chua_co_rule",
            "so_luong": len(impacted),
            "cot": impacted,
            "vi_sao": "Lineage cho thay cac cot nay bi BIEN DOI (khong phai IDENTITY)."
                      " Cot bi bien doi co the hong do chinh pipeline."})

    if not problems:
        return None
    return {"error": "CHUA DU DO PHU BAT BUOC - khong nop duoc.",
            "thieu": problems,
            "huong_dan": "Goi add_rules bo sung rule cho cac cot tren (gom tron mot stage vao"
                         " MOT lan goi), roi submit_rules_for_verification lai."}


def _impacted_columns(dataset_name):
    """Ten cac cot dau ra ma lineage ghi nhan co phep bien doi khac IDENTITY."""
    fields = (get_column_lineage(dataset_name) or {})
    out = set()
    for name, info in fields.items():
        if name.startswith("__"):
            continue
        transformation = (info or {}).get("transformation") or ""
        if transformation and transformation.upper() != "IDENTITY":
            out.add(name)
    return out


def _coverage_regression(rules: list, schema: dict, dataset_name: str):
    """Refuse a save that covers fewer columns than the rule set already published.

    Returns an error dict the agent can act on, or None when the save may proceed. Never raises:
    if the currently-active set cannot be read, there is nothing to regress against and the save
    is allowed through.
    """
    # Do phu tinh theo cap (stage, column): cung mot cot o hai checkpoint la HAI muc phu khac
    # nhau, khong phai trung lap - xem javadoc DqRule.
    covered = {(r.get("stage") or DEFAULT_STAGE, r.get("column")) for r in rules if r.get("column")}
    all_cols = set(schema.keys()) if schema else set()

    try:
        active = get_active_rules(_dq_key(dataset_name)) or {}
    except Exception:  # no baseline readable => nothing to protect
        return None

    prev_covered = {(r.get("stage") or DEFAULT_STAGE, r.get("column"))
                    for r in active.get("rules", []) if r.get("column")}
    if not prev_covered:
        return None

    # So do phu THEO TUNG STAGE, va chi so o nhung stage co mat o CA HAI ban. Mot stage moi
    # xuat hien (dev vua them checkpoint) thi khong co gi de regress; nguoc lai mot stage da
    # bi go khoi pipeline khong duoc tinh la mat do phu. So tong hai ban khong so sanh duoc:
    # chuyen tu mo hinh mot diem do sang nhieu diem do lam doi han don vi dem.
    def by_stage(pairs):
        out = {}
        for st, c in pairs:
            out.setdefault(st, set()).add(c)
        return out

    now_s, prev_s = by_stage(covered), by_stage(prev_covered)
    shared = set(now_s) & set(prev_s)
    lost = sorted(f"{st}.{c}" for st in shared for c in (prev_s[st] - now_s[st]))
    if not lost:
        return None

    covered_cols = {c for _st, c in covered}
    missing = sorted(all_cols - covered_cols) if all_cols else lost
    return {
        "error": "TU CHOI LUU: bo rule nay BAO PHU IT COT HON bo dang active - se lam mat do phu da co.",
        "covered_now": sorted(f"{st}.{c}" for st, c in covered),
        "covered_before": sorted(f"{st}.{c}" for st, c in prev_covered),
        "columns_lost": lost,
        "columns_still_uncovered": missing,
        "huong_dan": (
            f"Cac cap (stage, cot) bi mat so voi ban active: {', '.join(lost)}. "
            "Hay add_rules bu lai dung stage do roi goi submit_rules_for_verification lai. "
            "KHONG duoc luu bo kem hon o mot checkpoint da tung duoc phu."
        ),
    }


def run_agent(dataset_name: str, yaml_path: str = None, feedback: str = None):
    print(f"--- GEN RULES AGENT: {dataset_name} ---")
    dq_key = _dq_key(dataset_name)
    # business_metadata nap NGAY tai day chu khong doi build_context_prompt: chot do phu
    # bat buoc (_required_coverage) doc tu state, va neu key nay vang thi chot tro thanh
    # vo hieu trong IM LANG - dung kieu that bai ma ca framework nay duoc lam de tranh.
    business_metadata, _metadata_source = pipeline_config.load_business_metadata(yaml_path)
    state = {"lineage_hash": None, "rules": [], "candidate": None,
             "business_metadata": business_metadata}

    # Ca hai tool duoi day doc /dq/rules/{key}/active.json tren HDFS - CUNG nguon ma
    # Orchestrator, Execute Rules va job Java doc. Truoc day chung doc file cuc bo trong
    # STATE_DIR, va do la mot nguon su that THU HAI: trong driver pod thu muc do ephemeral
    # (Dockerfile tao rong, .dockerignore loai state/ ra khoi image), nen moi lan chay chung
    # deu tra "0 rule active" va "lineage da doi" bat ke HDFS dang co gi - day agent ve phia
    # sinh lai rule mot cach vo co.

    def _tool_count_active_rules():
        active = get_active_rules(dq_key) or {}
        return {"active_rule_count": len(active.get("rules", []))}

    def _tool_check_lineage_changed():
        new_hash = _compute_lineage_hash(dataset_name)
        state["lineage_hash"] = new_hash
        active = get_active_rules(dq_key) or {}
        stored_hash = active.get("lineage_hash")
        return {"lineage_changed": stored_hash != new_hash}

    def _tool_check_volume_anomaly():
        return tool_check_volume_anomaly(dataset_name)

    def _tool_add_rule(rule_id, column, description, failed_sql_expr, importance_weight,
                       stage=DEFAULT_STAGE, kind=None, observed_expr=None, expected=None,
                       observed_unit=None):
        # Di qua dung duong cua add_rules: mot noi kiem, mot noi ghi de. Truoc day hai ham
        # nay copy truong bang hai danh sach khac nhau va ca hai deu lac hau so voi schema.
        return _tool_add_rules([{
            "stage": stage or DEFAULT_STAGE,
            "rule_id": rule_id, "column": column, "description": description,
            "failed_sql_expr": failed_sql_expr, "importance_weight": importance_weight,
            "kind": kind, "observed_expr": observed_expr, "expected": expected,
            "observed_unit": observed_unit,
        }])

    def _tool_add_rules(rules):
        """Them NHIEU rule mot lan.

        Bat buoc phai co, khong phai tien nghi: do duoc tren cum that, add_rule tung cai mot
        lam agent cham tran LLM_MAX_ITERATIONS (20) va THAT BAI 4/4 lan khi phai phu 9 checkpoint
        - loi "Agent khong goi tool ket thuc sau 20 vong lap". Mot lan goi cho ca mot stage thi
        vua du vong lap.
        """
        if not isinstance(rules, list) or not rules:
            return {"error": "Tham so 'rules' phai la mang khong rong."}
        added, replaced, bad = 0, 0, []
        index = {(r["stage"], r["rule_id"]): i for i, r in enumerate(state["rules"])}
        # Doi chieu cot NGAY TAI DAY thay vi doi den khau nop.
        #
        # Muc 0 cua context da co bang tra cuu cot->checkpoint som nhat, dat o 7% dau prompt,
        # noi ro "trend_points=tf_capacity". Agent van dat chin rule tf_capacity vao agg_daily,
        # qua ca ba luot regen (22/09/2026). Them chu khong chua duoc: business metadata mo ta
        # bang DAU RA nen moi cot doc len deu nghe nhu thuoc ve stage tong hop, va suy luan
        # nghia thang suy luan lich su.
        #
        # Cai sua duoc la VI TRI kiem tra. Truoc day add_rules nhan tat, chi submit_rules moi
        # doi chieu - nen loi ve thanh mot buc tuong sau 40 lan goi, va agent phai phat lai tu
        # dau. Kiem ngay tai lan goi thi loi ve dung luc, tung cai, va KEM SAN cau tra loi:
        # stage nao moi dung. Ba luot regen truoc do khong luot nao bi tieu vao viec doan.
        stage_cols_now, stage_order_now = {}, []
        try:
            stage_cols_now, stage_order_now = load_stage_columns(dq_key)
        except Exception:
            pass
        first_stage_of = {}
        for st in stage_order_now or []:
            for col in st.get("columns") or []:
                first_stage_of.setdefault(col, st.get("stage"))
        # `no_value_when` la NGU NGHIA do metadata khai: cot nay vang gia tri trong tinh huong
        # nao. Khong phai rule - no khong noi phai viet gi, chi noi du lieu that ra sao.
        #
        # Can cho nay vi mot rule `col IS NULL` tran tren cot nhu vay bao hong dung bang so
        # dong ma du lieu HOAN TOAN dung. Do duoc 22/09/2026 tren cell_capacity_daily: sau lan
        # sinh rule lien tiep deu dinh, days_to_congestion 71.71%, longest_incident_hours
        # 71.52%, traffic_z_score 40.91% - va ca ba deu co mot cau trong mo ta noi ro khi nao
        # cot khong co gia tri. Them chu vao prompt khong chua duoc; dat phep kiem ngay tai
        # cho ghi thi chua duoc, dung nhu voi loi dat sai stage.
        no_value_when = {}
        for col_meta in ((state.get("business_metadata") or {}).get("columns") or []):
            when = str(col_meta.get("no_value_when") or "").strip()
            if when:
                no_value_when[col_meta.get("name")] = " ".join(when.split())
        for i, r in enumerate(rules):
            if not isinstance(r, dict):
                bad.append(f"phan tu[{i}] khong phai object"); continue
            missing = [k for k in RULE_REQUIRED_FIELDS if not r.get(k)]
            if missing:
                bad.append(f"phan tu[{i}] ({r.get('rule_id','?')}) thieu: {', '.join(missing)}")
                continue

            # rule_id phai la DINH DANH. No di vao ten cot metric cua truy van aggregate, vao khoa
            # (stage, rule_id) cua kho, vao khoa dong cua bang tren dashboard va vao cau SQL
            # tra cuu bang Iceberg. Da lot that 22/09/2026: "TS_DL_PRB Recomputed_RANGE" -
            # co dau cach - qua het moi chot va chi lo ra khi doc bao cao bang mat.
            rid = str(r.get("rule_id") or "")
            if not re.fullmatch(r"[A-Za-z0-9_]+", rid):
                bad.append(
                    f"phan tu[{i}]: rule_id {rid!r} khong phai dinh danh hop le."
                    " Chi dung chu khong dau, chu so va gach duoi - khong dau cach,"
                    " khong dau gach ngang, khong ky tu co dau.")
                continue
            # Cac truong TUY CHON phai duoc giu lai. Truoc day chi copy 6 truong bat buoc
            # nen kind/observed_expr/expected bi vut am tham: agent sinh dung, schema tool
            # nhan dung, nhung rule luu xuong lai mat - va rule muc tap bi ha thanh rule muc
            # dong ma khong ai bao gi.
            # Chi chan khi BIET chac stage do co nhung cot nao. Pipeline chua co manifest thi
            # stage_cols_now rong, va khi do chan het la chan oan.
            col, stage = r.get("column"), r.get("stage")
            # TEN STAGE sai chinh ta thi chan truoc, vi nhanh "khong biet stage nay" ben duoi
            # se cho no di qua. Da lot that 22/09/2026: agent viet "wh_hierarchy" thay vi
            # "wd_hierarchy", bon rule do khong thuoc checkpoint nao nen khong bao gio chay,
            # va Verify phai tieu mot luot regen chi de bat mot loi danh may.
            if stage_cols_now and stage not in stage_cols_now:
                bad.append(
                    f"phan tu[{i}] ({r.get('rule_id','?')}): stage '{stage}' KHONG co trong"
                    f" manifest. Stage hop le: {', '.join(sorted(stage_cols_now))}.")
                continue
            if stage in stage_cols_now and col not in stage_cols_now[stage]:
                where = first_stage_of.get(col)
                if where:
                    bad.append(
                        f"phan tu[{i}] ({r.get('rule_id','?')}): cot '{col}' KHONG co o stage"
                        f" '{stage}' - no xuat hien tu stage '{where}' tro di."
                        f" Gui lai rule nay voi stage='{where}'.")
                else:
                    bad.append(
                        f"phan tu[{i}] ({r.get('rule_id','?')}): cot '{col}' KHONG ton tai o"
                        " BAT KY stage nao cua pipeline. Doi chieu lai bang tra cuu o muc 0"
                        " - dung viet rule cho cot khong co that.")
                continue

            expr = str(r.get("failed_sql_expr") or "")
            if col in no_value_when and bare_null_check(expr, col):
                bad.append(
                    f"phan tu[{i}] ({r.get('rule_id','?')}): cot '{col}' KHONG CO GIA TRI khi"
                    f" {no_value_when[col]} - do la du lieu dung, khong phai loi."
                    " Bo '%s IS NULL' di: dung \"%s IS NOT NULL AND <ngoai mien>\" neu chi"
                    " muon kiem mien gia tri, hoac dat dieu kien truoc \"<cot co> AND %s IS"
                    " NULL\" neu muon bat truong hop dang le phai co gia tri."
                    % (col, col, col))
                continue

            entry = {k: r[k] for k in RULE_REQUIRED_FIELDS}
            for optional in RULE_OPTIONAL_FIELDS:
                if r.get(optional):
                    entry[optional] = r[optional]
            # GHI DE theo (stage, rule_id) chu khong append. Prompt va moi thong diep tu choi
            # deu bao "sua bang add_rules", nhung neu day chi la append thi sua mot rule hong
            # chi tao them mot ban trung - khong sua duoc gi. Agent het duong nen quay sang
            # clear_accumulated_rules, vut ca tram rule dung vi vai rule hong. Da dinh that.
            key = (entry["stage"], entry["rule_id"])
            if key in index:
                state["rules"][index[key]] = entry
                replaced += 1
            else:
                index[key] = len(state["rules"])
                state["rules"].append(entry)
                added += 1
        per_stage = {}
        for r in state["rules"]:
            per_stage[r["stage"]] = per_stage.get(r["stage"], 0) + 1
        out = {"added": added, "accumulated_count": len(state["rules"]),
               "per_stage": per_stage}
        if replaced:
            out["replaced"] = replaced
            out["note"] = (f"{replaced} rule trung (stage, rule_id) da duoc GHI DE bang"
                           " ban moi. Day la cach sua mot rule bi tu choi.")
        # Nhac ngay trong ket qua tool, khong phai trong system prompt. Prompt DA dan "gom moi
        # stage thanh mot lan goi" ma agent van chia lo 9-4-3-3-2 roi cham tran vong lap. Mot
        # loi nhac dat dung luc agent vua chia lo nho thi kho lo hon.
        uncovered = [st.get("stage") for st in (stage_order_now or [])
                     if st.get("stage") not in per_stage]
        if added < SMALL_BATCH_THRESHOLD and uncovered:
            out["iteration_budget_warning"] = (
                f"Lan goi nay chi them {added} rule ma con {len(uncovered)} checkpoint chua co "
                f"rule nao ({', '.join(uncovered[:UNCOVERED_STAGES_SHOWN])}). So vong lap CO HAN"
                " va het luot thi bo rule dang xay bi danh dau truncated. Hay gom TRON rule cua"
                " mot checkpoint vao MOT lan goi add_rules.")
        if bad:
            out["rejected"] = bad
            out["rejected_note"] = (f"{len(bad)} rule KHONG duoc nhan va KHONG nam trong"
                                    f" {len(state['rules'])} rule da tich luy. Sua theo tung"
                                    " dong o tren roi goi lai add_rules cho DUNG nhung rule do.")
        return out

    def _tool_verify_accumulated_rules():
        ok, issues = tool_verify_rules(state["rules"])
        return {"ok": ok, "issues": issues, "accumulated_count": len(state["rules"])}

    def _tool_check_rules_against_schema():
        schema = get_dataset_schema(dataset_name)
        stage_cols, _order = load_stage_columns(dq_key)
        hard_issues, soft_warnings = tool_schema_grounding(state["rules"], schema, stage_cols)
        return {
            "ok": len(hard_issues) == 0,
            "hard_issues": hard_issues,
            "soft_warnings": soft_warnings,
            "schema_columns": sorted(schema.keys()) if schema else [],
            "stages_known": sorted(stage_cols.keys()),
        }

    def _tool_dry_run_rules():
        return {"dry_run": "not_available", "reason": "Pipeline này chạy trên Iceberg/HDFS qua Spark, chưa có dry-run engine (DuckDB/Delta) tương đương. Dựa vào verify_accumulated_rules + check_rules_against_schema."}

    def _tool_clear_accumulated_rules():
        """Xoa sach - va TU CHOI khi da tich duoc mot bo dang ke.

        Prompt da cam, ket qua tool da canh bao, agent van xoa: bi submit tu choi vi 5 rule
        thieu observed_unit thi no vut ca 116 rule roi xay lai tu dau, va thuong het vong lap
        truoc khi kip. Vong ngoai lam viec nay ba luot lien. Nen cam tai tool, khong phai o
        loi khuyen: gio add_rules ghi de duoc theo rule_id nen sua tung rule LA duong co that.
        """
        count = len(state["rules"])
        if count >= CLEAR_REFUSE_THRESHOLD:
            return {"error": f"TU CHOI XOA: dang giu {count} rule. Bi tu choi vai rule thi SUA"
                             " chung, dung vut het - goi add_rules voi DUNG (stage, rule_id)"
                             " bi neu ten va ban da sua, no se GHI DE ban cu. Xoa sach chi"
                             " dung khi ca bo sai tu goc, luc bo con nho.",
                    "accumulated_count": count}
        state["rules"] = []
        return {"accumulated_count": 0}

    def _tool_save_accumulated_rules():
        rules = state["rules"]
        ok, issues = tool_verify_rules(rules)
        if not ok:
            return {"error": "Rules KHÔNG PASS verify nội bộ, từ chối lưu.", "issues": issues}

        schema = get_dataset_schema(dataset_name)
        stage_cols, _order = load_stage_columns(dq_key)
        hard_issues, _soft = tool_schema_grounding(rules, schema, stage_cols)
        if hard_issues:
            return {"error": "Rules THAM CHIẾU CỘT KHÔNG TỒN TẠI, từ chối lưu.", "issues": hard_issues}

        # Regression guard. verify/schema-grounding check that each rule is VALID, neither checks
        # that the SET is adequate -- so a regen could (and twice did) publish a thinner set over
        # a better one, destroying coverage that a previous run had achieved. Execute Rules only
        # catches that afterwards, by which point active.json is already overwritten.
        # Returned as a tool result rather than an exception so the agent can add the missing
        # rules and call save again within the same run.
        regression = _coverage_regression(rules, schema, dataset_name)
        if regression:
            return regression

        # Do phu BAT BUOC: kiem sau cung vi no la dieu kien nang nhat, va chi co nghia khi
        # rule da qua duoc cac chot tren.
        required = _required_coverage(rules, state.get("business_metadata"), dataset_name, dq_key)
        if required:
            return required

        lineage_hash = state["lineage_hash"] or _compute_lineage_hash(dataset_name)
        generation_run_id = str(uuid.uuid4())
        saved_at = datetime.now(timezone.utc).isoformat()

        record = {
            "dataset_name": dataset_name,
            "generation_run_id": generation_run_id,
            "generated_at": saved_at,
            "lineage_hash": lineage_hash,
            "rules": rules,
        }
        # KHONG ghi xuong HDFS o day - CO Y. Bo rule chi duoc publish SAU khi verify_rules_agent
        # tham dinh xong (yeu cau R3: khong ghi rule chua duoc verify xuong kho). Orchestrator
        # cam ban ghi nay trong bo nho, chuyen sang khau verify, va CHINH khau verify moi goi
        # publish_rules. Xem orchestrator.dispatch_verify_rules.
        state["candidate"] = record
        per_stage = {}
        for r in rules:
            per_stage[r.get("stage", DEFAULT_STAGE)] = per_stage.get(r.get("stage", DEFAULT_STAGE), 0) + 1
        return {
            "submitted_for_verification": True,
            "saved": False,
            "note": "Rule CHUA duoc luu. Buoc tiep theo la tham dinh; chi khi dat moi ghi xuong.",
            "rules_count": len(rules),
            "per_stage": per_stage,
            "generation_run_id": generation_run_id,
        }

    def _tool_skip_generation(reason):
        return {"skipped": True, "reason": reason}

    tool_dispatch = {
        "count_active_rules": _tool_count_active_rules,
        "check_lineage_changed": _tool_check_lineage_changed,
        "check_volume_anomaly": _tool_check_volume_anomaly,
        "add_rule": _tool_add_rule,
        "add_rules": _tool_add_rules,
        "verify_accumulated_rules": _tool_verify_accumulated_rules,
        "check_rules_against_schema": _tool_check_rules_against_schema,
        "dry_run_rules": _tool_dry_run_rules,
        "clear_accumulated_rules": _tool_clear_accumulated_rules,
        "submit_rules_for_verification": _tool_save_accumulated_rules,
        "skip_generation": _tool_skip_generation,
    }

    system_prompt = build_system_prompt()
    user_prompt = build_context_prompt(dataset_name, yaml_path, dq_key)
    if feedback:
        # Regen loop (Orchestrator -> dispatch_gen_rules(feedback=...)): Execute Rules judged
        # the previous rule set insufficient -- fold its feedback into THIS run's first prompt
        # rather than a separate turn, so the agent has it from the very first tool call.
        user_prompt += f"\n=== 4. PHAN HOI TU LAN CHECK TRUOC (Orchestrator yeu cau regen) ===\n{feedback}\n"

    try:
        stop_tool, stop_result, transcript = run_tool_calling_agent(
            system_prompt=system_prompt,
            user_prompt=user_prompt,
            tools_schema=TOOLS_SCHEMA,
            tool_dispatch=tool_dispatch,
            stop_tools={"submit_rules_for_verification", "skip_generation"},
        )

        if stop_tool == "skip_generation":
            print(f"=> Agent quyết định SKIP: {stop_result.get('reason')}")
        else:
            print(f"=> Agent NỘP THẨM ĐỊNH: {stop_result.get('rules_count')} rule "
                  f"({stop_result.get('per_stage')})")

        # Gan ban ghi ung vien SAU khi vong lap ket thuc, khong phai trong ket qua tool: neu de
        # trong ket qua tool thi ca bo rule bi dua nguoc vao transcript cua LLM - ton token vo
        # ich va co the day agent cham tran vong lap.
        if stop_tool == "submit_rules_for_verification":
            stop_result = {**stop_result, "_candidate": state.get("candidate")}

        print("--- GEN RULES AGENT HOÀN TẤT ---")
        return stop_tool, stop_result, transcript

    except AgentLoopError as e:
        print(f"LỖI: {e}")
        # VOT LAI thay vi vut het. Da do that 21/09/2026: sau hai vong bi Verify tu choi, vong
        # ba cham tran 60 vong lap; toan bo rule da tich luy bi vut, checkpoint tra PROCEED va
        # ca lan chay do 0 rule. Nhung rule do KHONG mat gia tri chi vi agent het luot noi -
        # chung van phai di qua dung cai cong tham dinh nhu moi bo rule khac, va Verify se noi
        # chung dat hay khong. Vot lai bien mot lan mat trang thanh mot bo rule co the chua du
        # nhung da duoc soi.
        rules = state.get("rules") or []
        if not rules:
            raise                      # khong co gi de vot
        salvaged = _tool_save_accumulated_rules()
        if not salvaged.get("submitted_for_verification"):
            # Rule tich luy khong qua noi self-check -> khong co gi dang cuu.
            print(f"   khong vot duoc: {salvaged.get('error')}")
            raise
        print(f"   VOT duoc {salvaged['rules_count']} rule dang xay do ({salvaged['per_stage']})"
              f" - chuyen sang tham dinh, danh dau truncated.")
        salvaged = {**salvaged, "truncated": True, "truncated_reason": str(e),
                    "_candidate": state.get("candidate")}
        return "submit_rules_for_verification", salvaged, []


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset-name", required=True, help="Physical dataset name registered in Marquez, e.g. /warehouse/<db>.db/<table>")
    parser.add_argument("--yaml-path", help="Override the configured business metadata YAML "
                                             "(default: local path from DQ_BUSINESS_METADATA_YAML, "
                                             "else /dq/metadata/<DQ_KEY>.yaml on HDFS)")
    args = parser.parse_args()

    run_agent(args.dataset_name, args.yaml_path)
