"""
Analysis agent (tool-calling, real agent) — root-cause / predictive analysis over the DQ
execution report that SubVoiceCallDailyJob.java publishes to /dq/reports/{key}/latest.json
(computed by a separate aggregate in dq/DqMetricQuery.java, see dq/DqReportWriter.java).

Ported from dq-lineage-pipeline/jobs/dq_core/analyze_results.py, adapted:
  - No Postgres: reads the report from /dq/reports/{key}/latest.json (HDFS) instead of
    dq_reports/dq_errors tables, and reads the rule descriptions/failed_sql_expr it needs for
    root-cause context from /dq/rules/{key}/active.json (the report itself only carries
    rule_id/column/violation stats, not the rule's description or SQL — that lives in the rule
    set, keyed by the same generation_run_id the report links back to).
  - save_analysis writes to /dq/analysis/{key}/latest.json + history/, same HDFS area/pattern
    as rules and reports, instead of the dq_analysis table.
  - decide_pipeline_action is NOT wired to sys.exit(1) here. In the old pipeline this ran
    inline in the same Airflow DAG, positioned before the next transform step, so HALT could
    genuinely stop the pipeline. Here the Spark job has ALREADY finished by the time this runs
    (it's a separate, later Python process reading a report the job already wrote) -- so the
    decision is recorded as a recommendation for whatever runs next (a future Orchestrator, or
    a human), not a live kill switch. Documented explicitly rather than silently pretending it
    has the same teeth as the original.

HAI PHAM VI, MOT AGENT
======================
Agent nay duoc Orchestrator goi o hai cho khac nhau, va no PHAI biet minh dang o cho nao:

  stage="src_calls" + stage_report={...}   MOT CHANG. Goi ngay tai checkpoint gate_<stage>,
                                           GIUA lan chay, job Spark dang nam cho. So lieu di
                                           THANG tu ctx cua job -- KHONG doc HDFS.
  stage=None                               CA LUOT CHAY. Goi o checkpoint post_report, doc
                                           /dq/reports/{key}/latest.json da gop moi chang.

Vi sao che do gate khong doc HDFS: DqGuard.java goi askAgent() o dong 201, nhung mai den dong
205-206 moi stageReports.add() va publishPartialReport(). Nghia la trong luc agent nay dang
suy nghi, chang DANG DUNG chua he co trong latest.json -- doc HDFS o gate se phan tich moi
chang TRU chang can phan tich. Nhan thang stage_report la cach duy nhat dung.

HAI HINH DANG REPORT
====================
DqReportWriter.buildPipelineReport ghi report_shape="per_stage": cap tai lieu chi co
worst_status + stages[], KHONG co "status" va KHONG co "rules" o muc goc. Ban cu doc
report["status"] / report["rules"] tra ve None va [] tren moi report thuc te -- agent chay
voi 0 rule vi pham du pipeline dang hong. Xem _read_report_shape.

Tra ve: LUON LUON mot dict (xem _result). Truoc day ham nay tra ve 4 hinh dang khac nhau --
dict loi, tuple 3, tuple 3 khac, tuple 4 -- va Orchestrator giai nen kieu tuple-4 nen hai
nhanh loi nem ValueError/KeyError roi bi except Exception cua agent_loop nuot thanh mot
thong bao chung chung. Nhanh STALE_REPORT chua bao gio chay toi dich.
"""
import json
import os
import sys
import uuid
from datetime import datetime, timezone

from agent_loop import AgentLoopError, run_tool_calling_agent
from dq_store import get_active_rules, get_latest_report
from lineage_client import get_column_lineage, get_downstream_datasets

AGENT_SYSTEM_PROMPT = """Bạn là AI Data Quality Analyst — AGENT THẬT cho hệ thống Data Lakehouse Viễn thông,
có quyền tự truy vấn dữ liệu qua tool.

Các tool khả dụng:
- get_dq_report(): lấy thông tin report DQ hiện tại (dataset_name, run_id, quality_score, total_rows, status).
- get_failed_rules(): lấy danh sách rule bị vi phạm (violation_count > 0) kèm description + failed_sql_expr.
- get_lineage(): lấy column lineage (nguồn gốc + phép biến đổi) của dataset.
- get_downstream_datasets(): lấy danh sách bảng hạ nguồn tiêu thụ dataset này.
- save_analysis(analysis_type, content): nộp bản phân tích. analysis_type là "ROOT_CAUSE" hoặc "PREDICTIVE".
  (Bản phân tích được Orchestrator ghi xuống kho, không phải bạn.)
- decide_pipeline_action(action, reasoning): KHUYẾN NGHỊ cuối cùng (KHÔNG dừng job thật — job Spark đã
  chạy xong trước khi bạn được gọi, đây là khuyến nghị ghi lại cho bước tiếp theo/con người). Tool KẾT THÚC.

=== NHIỆM VỤ ===
1. Gọi get_dq_report() trước để biết trạng thái hiện tại.
2. Nếu status là FAIL hoặc WARNING: phân tích ROOT-CAUSE — dùng get_failed_rules() + get_lineage() để
   xác định nguyên nhân gốc rễ. Nếu TOÀN BỘ phép biến đổi (transformation) của cột lỗi đều là "IDENTITY"
   (pass-through từ nguồn), kết luận nguyên nhân từ hệ thống nguồn (source/sensor/file). Nếu có biến đổi
   khác (CAST, aggregate...), phân tích kỹ logic biến đổi đó xem có phải nguyên nhân không. Đề xuất cách
   sửa cho kỹ sư dữ liệu.
3. Nếu status là PASS nhưng vẫn còn vài vi phạm nhỏ: dự báo PREDICTIVE — dùng get_downstream_datasets()
   để xem KPI nào có nguy cơ bị tính sai nếu dữ liệu lỗi này lọt xuống hạ nguồn. TUYỆT ĐỐI KHÔNG bịa tên
   bảng hạ nguồn không có trong kết quả trả về của get_downstream_datasets() — nếu danh sách rỗng, chỉ
   cảnh báo chung về KPI nghiệp vụ, không nêu tên bảng cụ thể.
4. Luôn gọi save_analysis(analysis_type, content) để lưu bản phân tích (tiếng Việt, súc tích, chuyên
   nghiệp, khoảng 150-300 chữ).
5. BẮT BUỘC kết thúc bằng đúng một lần gọi decide_pipeline_action(action, reasoning).

=== ĐỌC RULE MỨC TẬP (kind = "dataset") ===
Mỗi rule trong report có trường `kind`. Với `kind` = "row", `violation_count` là SỐ DÒNG hỏng
thật. Với `kind` = "dataset", rule hỏi một câu về CẢ BẢNG (khoá còn duy nhất không, tổng có
vượt ngưỡng không) — câu trả lời chỉ có đúng/sai, nên framework quy ước vi phạm = 100% và
`violation_count` = toàn bộ số dòng.

KHÔNG đọc con số đó là "toàn bộ dòng đều hỏng". Con số thật nằm ở `observed_value`, và ngưỡng
kỳ vọng ở `expected`. Ví dụ: rule duy nhất trên call_id báo violation_pct = 100 và
violation_count = 480, nhưng `observed_value` = 9 và `expected` = "= 0" nghĩa là có 9 dòng
trùng khoá, không phải 480 dòng hỏng. Nói nhầm chỗ này ở một checkpoint đang chờ người quyết
định là đẩy người ta tới quyết định sai.

Khi mô tả một vi phạm `kind` = "dataset", luôn trích `observed_value` và `expected`.

=== VỀ decide_pipeline_action ===
CHỈ chọn HALT khi có BẰNG CHỨNG CỤ THỂ (từ các tool tra cứu ở trên) cho thấy rủi ro nghiêm trọng tới dữ
liệu hạ nguồn mà quality_score/threshold không phản ánh đủ. Nếu report PASS với lỗi không đáng kể, hãy
chọn CONTINUE.
"""

#: Phu vao AGENT_SYSTEM_PROMPT khi Orchestrator goi TAI mot checkpoint gate_<stage>.
SCOPE_STAGE_PROMPT = """

=== PHẠM VI LẦN NÀY: MỘT CHẶNG ({stage}) ===
Bạn đang được gọi NGAY TẠI checkpoint gate_{stage}, GIỮA lần chạy. Job Spark đang nằm chờ.

- get_dq_report() trả về số đo của ĐÚNG chặng {stage}, không phải cả pipeline.
- get_failed_rules() chỉ liệt kê rule vi phạm TẠI chặng {stage}.
- KHÔNG suy diễn về các chặng SAU — chúng chưa chạy, bạn không có số liệu nào về chúng.
- Mọi kết luận phải nói rõ đang nói về chặng {stage}.

Khuyến nghị của bạn LẦN NÀY CÓ HIỆU LỰC THẬT: Orchestrator sẽ chuyển nó thành quyết định
checkpoint trả về cho job đang chờ, và HALT_AT_WRITE thật sự chặn được lệnh ghi. Đây không còn
là ghi chú cho người đọc sau. Cân nhắc đúng mức, và viết NGẮN (100-200 chữ) — mỗi giây bạn suy
nghĩ là một giây cluster nằm không.
"""

#: Phu vao AGENT_SYSTEM_PROMPT khi Orchestrator goi o post_report (het lan chay).
SCOPE_RUN_PROMPT = """

=== PHẠM VI LẦN NÀY: CẢ LƯỢT CHẠY ===
Report bạn đọc đã gộp TẤT CẢ các chặng. Mỗi rule vi phạm có trường `stage` cho biết nó hỏng ở
chặng nào — khi mô tả một vi phạm, LUÔN nêu rõ chặng.

Giá trị riêng của bạn ở phạm vi này là thứ không checkpoint lẻ nào thấy được: CHUỖI lỗi đi qua
nhiều chặng. Cùng một cột hỏng ở src_calls rồi còn hỏng ở final nghĩa khác hẳn với hỏng ở đúng
một chặng — chặng đầu là nguồn, các chặng sau là lan truyền. Hãy tìm và nói ra quan hệ đó.

Job Spark của lượt này ĐÃ chạy xong trước khi bạn được gọi, nên khuyến nghị lần này là ghi chú
lại cho bước tiếp theo/con người, KHÔNG dừng được gì. Viết 150-300 chữ.
"""

TOOLS_SCHEMA = [
    {"type": "function", "function": {
        "name": "get_dq_report",
        "description": "Lấy thông tin report DQ hiện tại đang được phân tích.",
        "parameters": {"type": "object", "properties": {}, "required": []},
    }},
    {"type": "function", "function": {
        "name": "get_failed_rules",
        "description": "Lấy danh sách rule bị vi phạm (violation_count > 0) trong report này, kèm description + failed_sql_expr.",
        "parameters": {"type": "object", "properties": {}, "required": []},
    }},
    {"type": "function", "function": {
        "name": "get_lineage",
        "description": "Lấy column lineage (nguồn gốc + phép biến đổi) của dataset trong report này.",
        "parameters": {"type": "object", "properties": {}, "required": []},
    }},
    {"type": "function", "function": {
        "name": "get_downstream_datasets",
        "description": "Lấy danh sách bảng hạ nguồn (downstream) tiêu thụ dataset này.",
        "parameters": {"type": "object", "properties": {}, "required": []},
    }},
    {"type": "function", "function": {
        "name": "save_analysis",
        "description": "Lưu bản phân tích (root-cause hoặc predictive) vào /dq/analysis/.",
        "parameters": {
            "type": "object",
            "properties": {
                "analysis_type": {"type": "string", "enum": ["ROOT_CAUSE", "PREDICTIVE"]},
                "content": {"type": "string", "description": "Nội dung phân tích, tiếng Việt."},
            },
            "required": ["analysis_type", "content"],
        },
    }},
    {"type": "function", "function": {
        "name": "decide_pipeline_action",
        "description": "Khuyến nghị cuối cùng (không dừng job thật). Tool KẾT THÚC.",
        "parameters": {
            "type": "object",
            "properties": {
                "action": {"type": "string", "enum": ["CONTINUE", "HALT"]},
                "reasoning": {"type": "string", "description": "Lý do cụ thể cho quyết định."},
            },
            "required": ["action", "reasoning"],
        },
    }},
]


def _result(outcome: str, action=None, reasoning=None, transcript=None, pending=None,
            message=None, stage=None) -> dict:
    """Hinh dang tra ve DUY NHAT cua analyze().

    outcome: "analyzed" | "skipped" | "stale_report" | "no_report"
    Mot hinh dang cho moi duong ra, ke ca duong loi. Ben goi doc outcome roi moi doc phan con
    lai - khong con phai doan xem lan nay tra ve tuple may phan tu hay mot dict loi.
    """
    return {"outcome": outcome, "action": action, "reasoning": reasoning,
            "transcript": transcript or [], "pending": pending or [],
            "message": message, "stage": stage}


def _read_report_shape(report: dict) -> tuple:
    """Doc (status, rules) tu BAT KY hinh dang report nao. -> (status, [rule kem 'stage'])

    DqReportWriter ghi ba hinh dang khac nhau len cung mot duong dan:
      per_stage       gop nhieu chang: co worst_status + stages[], KHONG co status/rules goc
      no_measurement  khong chang nao do duoc: co worst_status, khong co rules
      phang           mot chang duy nhat (duong afterWrite cu): co status + rules goc

    Doc nham hinh dang khong nem loi - no tra ve 0 rule vi pham, va agent ket luan "khong co
    van de gi" tren mot pipeline dang hong. Loai im lang, nen xu ly tuong minh ca ba.
    """
    if report.get("report_shape") == "per_stage":
        rules = []
        for st in report.get("stages") or []:
            stage_name = st.get("stage")
            for r in st.get("rules") or []:
                # Gan nhan chang vao TUNG rule: o pham vi ca luot chay, mot rule_id co the
                # xuat hien o nhieu chang va "rule X vi pham" la cau khong doc duoc.
                rules.append({**r, "stage": stage_name})
        return report.get("worst_status"), rules
    # Phang hoac no_measurement. worst_status la duong lui cho no_measurement, von khong co
    # "status" nhung van phai noi duoc rang lan chay do khong do duoc gi.
    return report.get("status") or report.get("worst_status"), list(report.get("rules") or [])


def _join_rule_details(raw_rules: list, rule_set: dict) -> list:
    """Loc rule vi pham va gan description + failed_sql_expr tu bo rule active.

    Report chi mang rule_id/column/so lieu vi pham; description va SQL nam ben rule set, khop
    nhau qua cung generation_run_id ma report tro toi.
    """
    rules_by_id = {r["rule_id"]: r for r in (rule_set or {}).get("rules", [])}
    failed = []
    for r in raw_rules:
        if r.get("violation_count", 0) > 0:
            full = rules_by_id.get(r.get("rule_id"), {})
            failed.append({
                **r,
                "description": full.get("description"),
                "failed_sql_expr": full.get("failed_sql_expr"),
            })
    return failed


def analyze(dq_key: str, dataset_name: str, expected_run_id: str = None,
            stage: str = None, stage_report: dict = None):
    """Phan tich chat luong du lieu. LUON tra ve dict (xem _result).

    stage + stage_report : PHAM VI MOT CHANG. Orchestrator goi tai checkpoint gate_<stage> va
        dua thang so do cua chang do sang. KHONG doc HDFS o duong nay -- xem docstring dau
        file: luc gate chay, chang dang dung chua duoc publish, nen HDFS chi co cac chang
        TRUOC no.
    stage=None           : PHAM VI CA LUOT CHAY. Doc /dq/reports/{key}/latest.json.

    expected_run_id: run_id ma ben goi tin la minh vua sinh ra. latest.json la bat cu thu gi
        lan chay Spark CUOI CUNG da ghi -- neu lan chay do khong xay ra (hoac that bai), ham
        nay doc mot report tu may hom truoc va phan tich no nhu du lieu hien tai. Da gap that
        khi thu: mot lan dry-run khong tao report nao, va no vui ve phan tich report cua lan
        chay that truoc do. O che do gate thi khong dung de kiem (so do den thang tu job, khong
        qua HDFS) nen chi dung lam nhan run_id cho ban ghi.
    """
    scope = f"gate {stage}" if stage else "ca luot chay"
    print(f"--- ANALYSIS AGENT: {dq_key} ({scope}) ---")

    if stage:
        if not stage_report:
            return _result("no_report", stage=stage, message=(
                f"Goi analyze(stage={stage!r}) nhung khong co stage_report kem theo. So do cua "
                "mot chang chi den tu ctx cua job Spark tai checkpoint gate_<stage>; khong co no "
                "thi khong co gi de phan tich (doc HDFS o day se ra cac chang TRUOC do)."))
        report = stage_report
        status = report.get("status")
        # Gan nhan chang cho dong bo voi pham vi ca luot chay, noi rule den tu nhieu chang.
        raw_rules = [{**r, "stage": stage} for r in (report.get("rules") or [])]
        run_id = expected_run_id
        scope_prompt = SCOPE_STAGE_PROMPT.replace("{stage}", stage)
    else:
        report = get_latest_report(dq_key)
        if not report:
            return _result("no_report", message=(
                f"Khong tim thay report tai /dq/reports/{dq_key}/latest.json - "
                "job Spark (Java) chua chay lan nao?"))
        if expected_run_id and report.get("run_id") != expected_run_id:
            msg = (f"Report tren HDFS thuoc run_id={report.get('run_id')} "
                   f"(executed_at={report.get('executed_at')}), KHONG phai run_id={expected_run_id} "
                   "vua chay. Co the lan chay vua roi khong tao report (that bai, hoac dry-run) - "
                   "tu choi phan tich report cu de tranh bao cao nham du lieu cu la hien tai.")
            print(f"CANH BAO: {msg}")
            return _result("stale_report", message=msg)
        status, raw_rules = _read_report_shape(report)
        run_id = report.get("run_id")
        scope_prompt = SCOPE_RUN_PROMPT

    rule_set = get_active_rules(dq_key) or {}
    failed_rules = _join_rule_details(raw_rules, rule_set)

    if status == "PASS" and not failed_rules:
        print("Report PASS và không có bất kỳ vi phạm nào. Bỏ qua phân tích để tiết kiệm chi phí LLM.")
        return _result("skipped", stage=stage,
                       message=f"{scope}: PASS voi 0 vi pham - khong can phan tich.")

    pending = []          # ban phan tich cho Orchestrator ghi

    def _tool_get_dq_report():
        return report

    def _tool_get_failed_rules():
        return {"errors": failed_rules}

    def _tool_get_lineage():
        return get_column_lineage(dataset_name)

    def _tool_get_downstream_datasets():
        return {"downstream_datasets": get_downstream_datasets(dataset_name)}

    def _tool_save_analysis(analysis_type, content):
        analysis_id = str(uuid.uuid4())
        record = {
            "analysis_id": analysis_id,
            "dq_key": dq_key,
            "report_run_id": run_id,
            # None o pham vi ca luot chay. Khong co truong nay thi chin ban phan tich cua chin
            # chang va mot ban tong the deu trong y het nhau khi doc lai tu kho.
            "stage": stage,
            "analysis_type": analysis_type,
            "content": content,
            "saved_at": datetime.now(timezone.utc).isoformat(),
        }
        # KHONG ghi thang. Orchestrator la ben duy nhat cham vao kho; ban phan tich di theo
        # gia tri tra ve cua analyze() va duoc ghi o do.
        pending.append(record)
        print(f"--- KẾT QUẢ PHÂN TÍCH ({analysis_type}) ---\n{content}\n-------------------------")
        return {"saved": True, "analysis_id": analysis_id}

    def _tool_decide_pipeline_action(action, reasoning):
        return {"action": action, "reasoning": reasoning}

    tool_dispatch = {
        "get_dq_report": _tool_get_dq_report,
        "get_failed_rules": _tool_get_failed_rules,
        "get_lineage": _tool_get_lineage,
        "get_downstream_datasets": _tool_get_downstream_datasets,
        "save_analysis": _tool_save_analysis,
        "decide_pipeline_action": _tool_decide_pipeline_action,
    }

    user_prompt = (
        f"Dataset: {dataset_name}\n"
        f"Run ID: {report.get('run_id')}\n"
        f"Status hiện tại: {status} | Quality score: {report.get('quality_score')} | Total rows: {report.get('total_rows')}\n"
        f"Số rule bị vi phạm: {len(failed_rules)}.\n"
        "Hãy bắt đầu phân tích theo đúng quy trình đã mô tả."
    )

    try:
        stop_tool, stop_result, transcript = run_tool_calling_agent(
            system_prompt=AGENT_SYSTEM_PROMPT,
            user_prompt=user_prompt,
            tools_schema=TOOLS_SCHEMA,
            tool_dispatch=tool_dispatch,
            stop_tools={"decide_pipeline_action"},
        )
    except AgentLoopError as e:
        print(f"LỖI: {e}")
        # RAISE chu khong sys.exit: SystemExit khong phai con cua Exception nen no xuyen qua
        # moi except Exception cua Orchestrator, giet luon tien trinh agent giua chung. Khi do
        # checkpoint khong con transcript, khong con tool_calls, va chi kip ghi mot dong
        # "checkpoint loi (SystemExit)". Da xay ra that o pipeline 79 cot: Gen Rules cham
        # LLM_MAX_ITERATIONS SAU khi da luu rule, va ca buoc tham dinh bi mat trang.
        raise

    action = stop_result.get("action")
    reasoning = stop_result.get("reasoning", "")
    print(f"=> KHUYẾN NGHỊ CỦA AGENT: {action} — {reasoning}")
    if action == "HALT":
        print("(Luu y: day chi la khuyen nghi ghi lai - job Spark cua lan chay nay da hoan tat truoc do,"
              " khong co gi de 'dung' o day. Se co y nghia that khi Orchestrator dieu phoi lien tuc.)")

    return action, reasoning, transcript, pending


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--dq-key", required=True, help="Ten ngan gon dung lam key thu muc /dq/, vd ten bang dau ra cua pipeline")
    parser.add_argument("--dataset-name", required=True, help="Physical dataset name registered in Marquez")
    args = parser.parse_args()

    analyze(args.dq_key, args.dataset_name)
