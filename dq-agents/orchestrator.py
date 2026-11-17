"""
Orchestrator — the single point of contact for both the user and the pipeline.

A real tool-calling agent (same agent_loop.py loop as every other agent here), NOT a
deterministic router. Its tools are DISPATCH tools, not workers themselves:

    get_pipeline_context()             -> the Orchestrator's OWN context: lineage/metadata/rules/
                                          report digest, read here rather than inside a subagent
    dispatch_gen_rules(feedback=None)  -> runs gen_rules_agent in-process
    dispatch_analysis()                -> runs analysis_agent in-process
    report_to_user(message)            -> stop tool, ends the turn

get_pipeline_context is the only NON-dispatch tool: the Orchestrator holds the context and
decides on it, then requests a subagent. It is deliberately a DIGEST (counts, names, status,
one changed/not-changed flag), not the payload -- see pipeline_context.py for why.

Subagents never call each other and never talk to the Spark pipeline directly. Verify Rules
can REFUSE a rule set -- that rejection comes back here, and the Orchestrator decides whether
to spend a regeneration attempt on it. Verify Rules never asks Gen Rules for anything itself.

THREE subagents, not four: gen / verify / analysis. Executing the rules is NOT an agent -- it
is the Spark job itself, at every dq.check() checkpoint. No tool here can submit a Spark run,
by design (a Spark job calls this agent; an agent submitting another Spark job is recursion).

Every subagent call returns a SHORT SUMMARY to the Orchestrator, not its full transcript, so
the Orchestrator's own context stays small across a multi-turn conversation.

Regen loop safety: dispatch_gen_rules enforces MAX_REGEN_ATTEMPTS in CODE, not just via the
system prompt -- an LLM can't be talked (by itself or a user) into looping forever.
"""
import os
import json
import sys
from datetime import datetime, timezone

# Chot quyen ghi: dq_store_write tu choi import neu bien nay khong phai "orchestrator".
# Dat o day, TRUOC moi import cua dq_*, vi kiem tra nam o tam import cua module do.
# Tien trinh con cua sub-agent khong di qua file nay nen khong bao gio co bien nay.
os.environ.setdefault("DQ_ROLE", "orchestrator")

from agent_loop import AgentLoopError, run_tool_calling_agent

import analysis_agent
import gen_rules_agent
import verify_rules_agent
from pipeline_context import build_digest
from subagent_result import SubagentResult
from tool_registry import ToolRegistry

from pipeline_config import DATASET_NAME, DQ_KEY

import dq_store_write                  # DUY NHAT o day: khong sub-agent nao duoc import
from dq_env import MAX_REGEN_ATTEMPTS  # gioi han vong regen, xem dq_env.py

#: Doi xung voi DqCheckpoint.Action ben Java. Lech nhau la hanh dong bi nuot am tham.
#: AWAIT_HUMAN la YEU CAU hoi nguoi, khong phai mot cau tra loi - nguoi khong chon no.
_AWAIT_HUMAN = "AWAIT_HUMAN"
_CHECKPOINT_ACTIONS = ("PROCEED", "PROCEED_ALL", "HALT_AT_WRITE", "HALT", _AWAIT_HUMAN)
#: Doi xung voi DqHumanDecision.STATUS_PENDING.
_PENDING = "pending"

SYSTEM_PROMPT = """Bạn là Orchestrator — đầu mối duy nhất giữa người dùng và framework Data Quality
cho pipeline {DQ_KEY}. Người dùng chỉ nói chuyện với BẠN; BA subagent (Gen Rules,
Verify Rules, Analysis) không bao giờ tự gọi nhau hay tự động chạy — BẠN là người quyết định gọi
cái nào, khi nào, và diễn giải kết quả cho người dùng.

VIỆC THỰC THI RULE KHÔNG PHẢI CỦA AGENT NÀO: chính job Spark chạy chúng tại mỗi checkpoint
dq.check(). Bạn không có tool nào submit được job Spark.

=== TOOL NẮM NGỮ CẢNH (tự bạn đọc, KHÔNG giao cho ai) ===
- get_pipeline_context(): BẠN tự lấy bức tranh hiện tại của pipeline — lineage có mấy cột và cột nào,
  business metadata mô tả những cột nào, ĐANG có bộ rule active nào (bao nhiêu rule, sinh lúc nào, phủ
  cột nào), report gần nhất ra sao (status/quality_score/run_id), và quan trọng nhất:
  lineage_changed_since_rules — lineage có ĐỔI kể từ lần sinh rule gần nhất không.
  Đây là BẢN TÓM TẮT để BẠN QUYẾT ĐỊNH, không phải dữ liệu đầy đủ; subagent vẫn tự đọc bản đầy đủ khi làm.

=== TOOL ĐIỀU PHỐI (dispatch — không tự làm việc, chỉ giao việc cho subagent) ===
- dispatch_verify_rules(): giao cho Verify Rules agent THẨM ĐỊNH bộ rule vừa sinh. Đây là agent
  KHÁC với Gen Rules. Chỉ khi nó duyệt thì rule mới được ghi xuống kho — Gen Rules KHÔNG còn
  quyền ghi. BẮT BUỘC gọi sau mỗi lần dispatch_gen_rules thành công, nếu không thì bộ rule vừa
  sinh nằm nguyên trong bộ nhớ và pipeline vẫn chạy với bộ rule cũ.
- dispatch_gen_rules(feedback=None): giao cho Gen Rules agent sinh/regen bộ rule DQ (đọc lineage +
  business metadata thật, tự self-check trước khi lưu). Truyền feedback khi đây là lần regen sau khi
  Verify Rules/Analysis phát hiện rule chưa đủ tốt. Có giới hạn cứng số lần regen (không phải do bạn
  tự giữ kỷ luật — nếu gọi quá giới hạn, tool sẽ trả lỗi max_regen_attempts_reached và bạn phải dừng
  vòng lặp, báo cho người dùng biết).
- dispatch_analysis(): giao cho Analysis agent đọc report + lineage, phân tích root-cause/predictive,
  đưa ra khuyến nghị CONTINUE hoặc HALT (chỉ là khuyến nghị ghi lại, không tự dừng gì cả).
- report_to_user(message): TRẢ LỜI người dùng. Tool KẾT THÚC lượt này.

=== LUỒNG MẶC ĐỊNH (linh hoạt theo yêu cầu người dùng, KHÔNG phải lúc nào cũng chạy hết) ===
0. LUÔN gọi get_pipeline_context() TRƯỚC TIÊN, trước khi giao việc cho bất kỳ subagent nào. Nắm ngữ
   cảnh rồi mới quyết định — đừng giao việc mù. Cụ thể, dùng nó để tránh làm thừa:
   - active_rules.exists = false  -> BẮT BUỘC dispatch_gen_rules trước, chưa có rule thì không chạy được gì.
   - lineage_changed_since_rules = false VÀ đã có rule -> lineage KHÔNG đổi, thường KHÔNG cần sinh lại
     rule. (Sinh lại lúc này chỉ tốn thêm một vòng LLM.)
   - lineage_changed_since_rules = true -> rule đang dựa trên lineage CŨ, nên dispatch_gen_rules lại.
   - checkpoint_coverage.needs_regeneration = true -> dev vừa THÊM/BỚT/ĐỔI TÊN checkpoint trong code,
     manifest không còn khớp bộ rule. Checkpoint mới sẽ chạy với 0 rule nếu không sinh lại.
     -> BẮT BUỘC dispatch_gen_rules.
   - rules_verification.verified = false -> bộ rule đang có CHƯA qua thẩm định (thiếu verdict, hoặc
     verdict FAIL). Không được dùng. -> dispatch_gen_rules rồi dispatch_verify_rules.
   - lineage.available = false -> Marquez chưa có lineage (job chưa chạy lần nào). Báo người dùng
     biết rule sẽ chỉ dựa trên business metadata, đừng im lặng bỏ qua.
1. Nếu người dùng chỉ muốn sinh rule ("chỉ generate rule thôi") -> dispatch_gen_rules() +
   dispatch_verify_rules() (sinh xong mà không thẩm định thì KHÔNG có gì được lưu) rồi
   report_to_user() ngay, KHÔNG tự ý chạy Analysis nếu không được yêu cầu.
2. Nếu người dùng muốn kiểm tra DQ đầy đủ -> get_pipeline_context() -> dispatch_gen_rules() (nếu rule
   chưa có/cần cập nhật) -> dispatch_verify_rules() -> dispatch_analysis() -> report_to_user().
3. BẠN KHÔNG CHẠY RULE. Rule chỉ được thực thi bởi chính job Spark, tại mỗi checkpoint dq.check().
   Không có tool nào ở đây submit được job Spark — đó là chủ đích: job Spark gọi agent, agent mà
   submit thêm một job Spark nữa thì thành đệ quy. Muốn có report mới thì chạy pipeline.
   Lưu ý: status=FAIL trong report (rule ĐÃ chạy và bắt được lỗi) KHÔNG phải lý do để sinh lại rule —
   đó thường nghĩa là DỮ LIỆU thật sự có vấn đề; hãy dispatch_analysis() rồi báo người dùng.
4. LUÔN kết thúc bằng report_to_user(message) tóm tắt rõ ràng, súc tích những gì đã làm và kết quả.
"""

# Ten pipeline dien bang .replace chu khong f-string: prompt chua dau ngoac nhon
# cua cac vi du JSON, f-string se hieu nham thanh bieu thuc.
SYSTEM_PROMPT = SYSTEM_PROMPT.replace("{DQ_KEY}", DQ_KEY)

GATE_PROMPT_SUFFIX = """

=== BAN DANG O MOT CHECKPOINT GIUA PIPELINE (gate_<stage>) ===
So do cua checkpoint nay DA CO trong context (stage_report): so dong, diem chat luong, rule nao
vi pham bao nhieu. Viec cua ban CHI la:
  1. Doc diem va danh sach rule vi pham.
  2. Neu can nhin them du lieu that thi goi query_live_stage - no dang tro vao dung dataframe
     cua checkpoint nay.
  3. Ra quyet dinh kem ly do CU THE (ten rule, ty le vi pham, anh huong).

NOI "FAIL"/"WARNING" thi PHAI NEU NGUONG di kem. stage_report.thresholds cho biet nguong da
cham chinh chang nay: viet "diem 0.998 < nguong fail 0.999 -> FAIL", dung viet "0.998 (FAIL)".
Nguong co the khac mac dinh o tung lan chay, va mot phan quyet khong kem nguong thi nguoi doc
log sau nay khong cach nao doi chieu - ho chi thay mot con so cao bi dan nhan FAIL va ket luan
he thong tinh sai. Da xay ra that.

KHONG sinh rule, KHONG tham dinh rule o day: bo rule da duoc chot truoc khi pipeline bat dau va
khong doi giua chung lan chay. Cac tool do da bi go khoi danh sach cua ban.

=== NAM HANH DONG ===
  PROCEED        di tiep. Checkpoint SAU van hoi neu lai hong. Day la lua chon mac dinh.
  PROCEED_ALL    di tiep VA khong hoi nguoi nua trong ca luot chay nay. Chi dung khi ban chac
                 cac checkpoint sau se lo ra CUNG MOT nguyen nhan goc (vi du 15 dong imsi hong
                 o src_calls se hien lai thanh original_imsi o jn_map1).
  HALT_AT_WRITE  chay tiep het cac chang de chan doan day du, NHUNG cam lenh ghi. Day thuong la
                 lua chon DUNG khi du lieu xau: muc tieu cua he thong nay la khong ghi du lieu
                 rac xuong, khong phai dung job.
  HALT           dung ngay tai day. Chi khi chay tiep la lang phi ro rang.
  AWAIT_HUMAN    xin y kien nguoi - xem duoi.

=== KHI NAO XIN Y KIEN NGUOI ===
Truong `on_fail_policy` trong context noi ban duoc phep lam gi o checkpoint NAY:

  warn        chi duoc PROCEED kem canh bao ro trong reason. KHONG duoc goi
              request_human_decision, KHONG duoc HALT tru khi du lieu hong hoan toan.
  ask_human   duoc phep chan cho nguoi quyet. Neu du lieu xau den muc ban khong tu tin tu
              quyet: goi request_human_decision(summary, recommended, default_on_timeout)
              roi BAT BUOC ket thuc bang checkpoint_decision("AWAIT_HUMAN", ...).
              Job Spark se nam cho toi khi co nguoi tra loi.
  halt        du lieu khong dat la dung, khong hoi ai.

`human_prompts_silenced = true` nghia la nguoi da tra loi PROCEED_ALL o mot checkpoint truoc -
luc do on_fail_policy da bi ha ve "warn", dung hoi lai.

`write_already_blocked_by` co gia tri nghia la mot checkpoint truoc da cam ghi roi. Khong can
chan lai; cu PROCEED de chan doan not cac chang con lai.

Nghi ngo thi PROCEED va noi ro canh bao trong reason. Nhung neu on_fail_policy = ask_human va
du lieu that su xau, thi HOI NGUOI mo hon la doan - day la chinh cho co nguoi de hoi."""


def run(user_request: str, checkpoint: str = None, job_context: dict = None):
    """Chay Orchestrator.

    checkpoint != None nghia la dang duoc goi TU BEN TRONG job Spark (qua DqCheckpoint.java).
    """
    # Che do checkpoint: stdout la kenh giao thuc voi Java, moi log phai ra stderr.
    print(f"--- ORCHESTRATOR: {user_request!r} (checkpoint={checkpoint}) ---",
          file=sys.stderr if checkpoint else sys.stdout)
    # job_context la du lieu JOB SPARK gui sang (spark_app_id, stage, stage_report...), khac
    # han "context" von la digest do get_pipeline_context sinh. Truoc day no chi duoc nhet vao
    # prompt duoi dang text, nen tool nao can doc no mot cach co cau truc thi khong lay duoc.
    state = {"candidate": None, "regen_count": 0,
             "last_run_id": (job_context or {}).get("spark_app_id"),
             "context": None, "job_context": job_context or {},
             "attempts": []}

    def _record_attempt(kind: str, ok: bool, status: str, summary: str, detail: dict = None):
        """Mot luot sinh rule hoac tham dinh rule.

        regen_count truoc day chi song trong RAM cua tien trinh nay, nen sau khi job xong khong
        con cach nao biet agent da thu lai may lan va vi sao - bang dq_agent_run_attempts cua
        ban Postgres cu khong dung lai duoc. Ghi ra day de DqWarehouseWriter lam phang no.
        """
        d = detail or {}
        state["attempts"].append({
            "attempt_no": len(state["attempts"]) + 1,
            "kind": kind,
            "ok": bool(ok),
            "status": status,
            "summary": (summary or "")[:1000],
            "regen_count": state["regen_count"],
            "rules_generated_count": d.get("rules_saved_count"),
            "at": datetime.now(timezone.utc).isoformat(),
        })

    tools = ToolRegistry()

    @tools.tool(
        "Lay BAN TOM TAT ngu canh pipeline de TU QUYET DINH: lineage (so cot, ten cot, phu "
        "thuoc INDIRECT), business metadata (cot nao duoc mo ta), rule dang active (so luong, "
        "sinh luc nao, phu cot nao), report gan nhat (status/quality_score/run_id), va "
        "lineage_changed_since_rules. Goi TRUOC khi dispatch bat ky subagent nao. Nhanh, khong "
        "ton LLM.")
    def get_pipeline_context():
        # The Orchestrator reads this ITSELF rather than delegating - it is the context it
        # decides on. Kept as a digest so it can stay in context for the whole conversation;
        # the subagents still read the full lineage/YAML/report when they actually work.
        digest = build_digest(DQ_KEY, DATASET_NAME)
        state["context"] = digest
        return SubagentResult(
            ok=True, status="context_loaded",
            summary=(
                f"lineage: {digest['lineage'].get('column_count', 0)} cot"
                f" | rule active: {digest['active_rules'].get('count', 0)}"
                f" | report gan nhat: {digest['last_report'].get('status', 'chua co')}"
                f" | lineage doi ke tu lan sinh rule: {digest['lineage_changed_since_rules']}"
            ),
            detail=digest,
        )

    @tools.tool(
        "Giao cho Gen Rules agent sinh/regen bo rule DQ.",
        param_docs={"feedback": "Phan hoi cu the neu day la regen (vd rule nao thieu, cot "
                                   "nao chua duoc bao phu). Bo trong neu la lan sinh dau."})
    def dispatch_gen_rules(feedback: str = None):
        if feedback and state["regen_count"] >= MAX_REGEN_ATTEMPTS:
            summary = f"Da cham gioi han {MAX_REGEN_ATTEMPTS} lan regen, dung vong lap."
            _record_attempt("gen_rules", False, "max_regen_attempts_reached", summary)
            return SubagentResult(
                ok=False, status="max_regen_attempts_reached",
                summary=summary, detail={"regen_count": state["regen_count"]},
            )
        if feedback:
            state["regen_count"] += 1

        stop_tool, stop_result, _transcript = gen_rules_agent.run_agent(
            DATASET_NAME, feedback=feedback)

        if stop_tool == "skip_generation":
            _record_attempt("gen_rules", True, "skipped", stop_result.get("reason", ""), stop_result)
            return SubagentResult(
                ok=True, status="skipped", summary=stop_result.get("reason", ""),
                detail={**stop_result, "regen_count": state["regen_count"]},
            )
        # Rule CHUA duoc luu. Giu ban ung vien trong bo nho de chuyen sang khau tham dinh -
        # day la cho bao dam "khong ghi rule chua verify xuong kho": khong co duong nao khac
        # tu gen_rules den active.json nua.
        state["candidate"] = stop_result.pop("_candidate", None)
        n = stop_result.get("rules_count")
        summary = (f"Sinh {n} rule qua {len(stop_result.get('per_stage') or {})} checkpoint "
                   f"- CHUA luu, cho tham dinh")
        _record_attempt("gen_rules", True, "generated", summary, stop_result)
        return SubagentResult(
            ok=True, status="generated", summary=summary,
            detail={**stop_result, "regen_count": state["regen_count"],
                    "next": "PHAI goi dispatch_verify_rules truoc khi rule co hieu luc"},
        )

    @tools.tool("Giao cho Verify Rules agent tham dinh bo rule vua sinh. CHI khi dat thi rule "
                "moi duoc ghi xuong kho. PHAI goi sau dispatch_gen_rules.")
    def dispatch_verify_rules():
        candidate = state.get("candidate")
        if not candidate:
            return SubagentResult(
                ok=False, status="no_candidate",
                summary="Chua co bo rule nao cho tham dinh - hay goi dispatch_gen_rules truoc.",
                detail={},
            )
        from dq_store import get_stages
        # Verify Rules chi THAM DINH va tra verdict. Viec GHI nam o day, va chi o day.
        result = verify_rules_agent.run(DQ_KEY, DATASET_NAME, candidate, get_stages(DQ_KEY))
        approved = bool(result.get("approved"))
        gen_id = candidate.get("generation_run_id")

        # Mot stage bi cham FAIL nghia la rule cua CHECKPOINT DO da bi phan quyet la khong dung
        # duoc - ghi xuong kho la ghi rule da biet la hong. Nhung tu choi vo dieu kien lai tao
        # ra mot cai bay khac: het luot regen ma van khong ghi gi thi job chay voi 0 rule, te
        # hon han. Nen: con luot thi TU CHOI de Gen Rules sua; het luot thi ghi kem danh dau
        # degraded_stages de Java biet cac checkpoint do khong dang tin.
        failed_stages = sorted(st for st, v in (result.get("per_stage") or {}).items()
                               if str(v).upper() == "FAIL")
        if approved and failed_stages:
            if state["regen_count"] < MAX_REGEN_ATTEMPTS:
                summary = (f"Tham dinh dat tong the ({result.get('verdict')}) nhung "
                           f"{len(failed_stages)} checkpoint bi cham FAIL: "
                           f"{', '.join(failed_stages)}. KHONG ghi - hay dispatch_gen_rules"
                           f"(feedback=...) sua rule cua dung cac checkpoint do.")
                _record_attempt("verify_rules", False, "stage_failed", summary, result)
                return SubagentResult(ok=True, status="rejected", summary=summary,
                                      detail={**result, "failed_stages": failed_stages})
            candidate["degraded_stages"] = failed_stages
            result["verdict_doc"]["degraded_stages"] = failed_stages

        if approved:
            try:
                dq_store_write.promote(DQ_KEY, candidate, gen_id, result["verdict_doc"])
            except Exception as e:
                # Tham dinh DAT ma ghi hong thi KHONG duoc bao la xong: job se nap bo rule cu
                # va bao cao se noi doi. Giu candidate lai de con thu ghi lai.
                summary = f"Tham dinh DAT ({result.get('verdict')}) nhung GHI HONG: {e}"
                _record_attempt("verify_rules", False, "write_failed", summary, result)
                return SubagentResult(ok=False, status="write_failed", summary=summary,
                                      detail={**result, "error": str(e)})
            state["candidate"] = None          # da luu, khong cho luu lai
            summary = (f"Tham dinh DAT ({result.get('verdict')}) - Orchestrator da ghi "
                       f"{len(candidate.get('rules', []))} rule (verdict -> history -> active)")
            if failed_stages:
                summary += (f". LUU Y: het luot regen nen van ghi, nhung {len(failed_stages)}"
                            f" checkpoint bi danh dau degraded: {', '.join(failed_stages)}")
        else:
            summary = f"Tham dinh TU CHOI: {str(result.get('reason'))[:160]}"
        _record_attempt("verify_rules", approved, "promoted" if approved else "rejected",
                        summary, result)
        return SubagentResult(
            ok=True, status="promoted" if approved else "rejected",
            summary=summary, detail=result,
        )

    @tools.tool("Giao cho Analysis agent phan tich report DQ gan nhat.")
    def dispatch_analysis():
        # Pass the run_id the last execution reported, so Analysis can refuse to analyze a
        # report left behind by an OLDER run (observed in testing: a run that produced no
        # report at all, after which Analysis happily read one from days earlier).
        result = analysis_agent.analyze(DQ_KEY, DATASET_NAME,
                                        expected_run_id=state.get("last_run_id"))
        if result is None or result[0] is None:
            return SubagentResult(
                ok=True, status="skipped",
                summary="Report PASS voi 0 vi pham - khong can phan tich.",
            )
        action, reasoning, _transcript, pending = result
        # Analysis agent khong ghi. Ban phan tich no nop duoc ghi o day.
        for rec in pending or []:
            try:
                dq_store_write.save_analysis(DQ_KEY, rec, rec["analysis_id"])
            except Exception as e:
                print(f"CANH BAO: khong ghi duoc analysis {rec.get('analysis_id')}: {e}",
                      file=sys.stderr)
        if action == "STALE_REPORT":
            return SubagentResult(ok=False, status="stale_report", summary=reasoning)
        return SubagentResult(
            ok=True, status="analyzed", summary=reasoning,
            detail={"action": action, "reasoning": reasoning},
        )

    @tools.tool("Xin y kien NGUOI tai checkpoint nay khi du lieu xau den muc khong nen tu quyet. "
                "Ghi mot ban ghi cho roi TRA VE NGAY - sau do BAT BUOC ket thuc bang "
                "checkpoint_decision(\"AWAIT_HUMAN\", ...) de job Spark chuyen sang cho. "
                "CHI dung khi on_fail_policy = ask_human.",
                param_docs={
                    "summary": "Tom tat NGAN cho nguoi doc: hong cai gi, o dau, nang den muc nao.",
                    "recommended": "Hanh dong ban khuyen nghi: PROCEED | PROCEED_ALL | HALT_AT_WRITE | HALT.",
                    "default_on_timeout": "Ap dung neu sau nay bat han muc thoi gian va het gio. "
                                          "Voi muc tieu khong ghi rac thi thuong la HALT_AT_WRITE."})
    def request_human_decision(summary: str, recommended: str, default_on_timeout: str = "HALT_AT_WRITE"):
        # Hai truong nay do LLM sinh ra chu khong phai nguoi bam - nguoi chi chon tu bon nut
        # co san. Nen chung phai duoc kiem o day, truoc khi vao ban ghi: Java doc
        # default_on_timeout luc het gio, va mot gia tri la o do la mot quyet dinh khong ai ky.
        def _clean(value, fallback):
            act = str(value or "").strip().upper()
            return act if act in _CHECKPOINT_ACTIONS and act != _AWAIT_HUMAN else fallback

        recommended = _clean(recommended, "HALT_AT_WRITE")
        default_on_timeout = _clean(default_on_timeout, "HALT_AT_WRITE")
        job = state.get("job_context") or {}
        run_id = job.get("spark_app_id")
        stage = job.get("stage")
        if not run_id or not stage:
            return SubagentResult(
                ok=False, status="thieu_ngu_canh",
                summary="Khong biet run_id hoac stage - khong ghi duoc ban ghi cho.", detail={})

        stage_report = job.get("stage_report") or {}
        violated = [r for r in stage_report.get("rules", [])
                    if (r.get("violation_count") or 0) > 0]
        record = {
            "dq_key": DQ_KEY, "run_id": run_id, "stage": stage,
            "quality_score": stage_report.get("quality_score"),
            "quality_score_trusted": stage_report.get("quality_score_trusted"),
            "status":_PENDING, "stage_status": stage_report.get("status"),
            "total_rows": stage_report.get("total_rows"),
            "violated_rules": violated,
            "agent_summary": summary,
            "recommended": recommended,
            "options": ["PROCEED", "PROCEED_ALL", "HALT_AT_WRITE", "HALT"],
            "default_on_timeout": default_on_timeout,
            "requested_at": datetime.now(timezone.utc).isoformat(),
        }
        try:
            path = dq_store_write.request_decision(DQ_KEY, run_id, stage, record)
        except Exception as e:
            # Ghi hong thi KHONG duoc bao la da hoi: Java se cho mot ban ghi khong bao gio den.
            return SubagentResult(ok=False, status="ghi_hong",
                                  summary=f"Khong ghi duoc ban ghi cho: {e}", detail={})
        print(f"=== da ghi ban ghi cho quyet dinh -> {path}", file=sys.stderr)
        return SubagentResult(
            ok=True, status="pending",
            summary=f"Da ghi ban ghi cho tai {path}. Gio BAT BUOC goi "
                    f"checkpoint_decision(\"AWAIT_HUMAN\", ...) de job chuyen sang cho nguoi.",
            detail={"path": path, "recommended": recommended})

    @tools.tool("Tra loi nguoi dung, tom tat ket qua. Tool KET THUC.")
    def report_to_user(message: str):
        return {"message": message}

    def _make_live_tool(name: str):
        """Bo mot tool ma JOB cong bo thanh mot ham goi duoc.

        Khong biet truoc tool nao ton tai, va co y nhu vay: job tu cong bo kha nang cua no
        trong dong hello. Them mot DqTool o phia Java la agent dung duoc ngay, khong dong
        Python nao phai doi - do la diem khac so voi ban truoc, khi query_live_stage duoc
        viet cung o ca hai phia va hai ban do co the lech nhau.
        """
        def _call(**args):
            import live_channel
            try:
                resp = live_channel.request(name, args)
            except live_channel.LiveChannelError as e:
                return SubagentResult(ok=False, status="live_channel_error", summary=str(e))
            if not resp.get("ok"):
                return SubagentResult(ok=False, status="tool_failed",
                                      summary=str(resp.get("error"))[:400])
            res = resp.get("result")
            if isinstance(res, dict) and "rows" in res:
                summary = f"{len(res.get('rows', []))} dong tra ve tu du lieu dang chay"
            else:
                summary = f"{name} chay xong"
            return SubagentResult(ok=True, status="ok", summary=summary,
                                  detail=res if isinstance(res, dict) else {"result": res})
        return _call

    @tools.tool(
        "Tra quyet dinh ve cho job Spark dang cho. Tool KET THUC (chi dung o che do checkpoint).",
        param_schema={"action": {"type": "string",
                                 "enum": ["PROCEED", "PROCEED_ALL", "HALT_AT_WRITE",
                                          "HALT", "AWAIT_HUMAN"]}},
        param_docs={"reason": "Ly do cu the, ngan gon."})
    def checkpoint_decision(action: str, reason: str):
        # Phan giai o MOT cho, doi xung voi DqCheckpoint.Action.parse ben Java. Truoc day cho
        # nay ep moi thu khong phai HALT thanh PROCEED, nen them hanh dong moi la no bi nuot
        # am tham - dung loai loi nguy hiem nhat o day.
        act = str(action).strip().upper()
        if act not in _CHECKPOINT_ACTIONS:
            print(f"CANH BAO: hanh dong '{action}' khong hop le, ve PROCEED", file=sys.stderr)
            act = "PROCEED"
        return {"action": act, "reason": reason}

    if checkpoint:
        import live_channel
        base_tools = tools
        # Che do checkpoint: phai ket thuc bang checkpoint_decision de tra quyet dinh cho job
        # dang doi, nen KHONG duoc lo report_to_user.
        #
        # Che do checkpoint KHONG duoc lo report_to_user: phai ket thuc bang
        # checkpoint_decision de tra quyet dinh cho job dang doi.
        #
        # Khong con tool nao submit duoc Spark. Truoc day Execute Rules lam duoc, va no la
        # duong DUY NHAT trong ca he thong co the sinh ra mot lan chay Spark thu hai; no bi
        # go o day nhu mot lop chan. Nay ca agent do da bi xoa nen lop chan thanh vinh vien,
        # khong con phu thuoc vao viec ai do nho go tool.
        tools = tools.subset({"report_to_user", "request_human_decision"})
        system_prompt = SYSTEM_PROMPT + CHECKPOINT_PROMPT_SUFFIX
        stop_tools = {"checkpoint_decision"}

        # Nhanh gate_<stage>: CHOT BANG CODE chu khong bang loi dan trong prompt.
        #
        # Truoc day khong co nhanh nay nen 9 gate roi vao luong mac dinh, va agent goi
        # dispatch_execute_rules (da xoa) o giua pipeline - nguon cua nhung dong "Da chay DQ check:
        # status=None, quality_score=None" trong trace. Tool do da bi xoa han, nhung nhanh
        # nay van can: o gate, so da do xong roi va bo rule bat bien suot lan chay (Java nap
        # dung mot lan), nen sinh rule hay tham dinh lai o day deu vo nghia.
        if checkpoint.startswith("gate_"):
            # Nhanh gate_: go gen/verify (rule da chot truoc khi job chay), nhung TRA LAI
            # request_human_decision - day la cho duy nhat duoc phep xin y kien nguoi.
            tools = base_tools.subset({"report_to_user",
                                       "dispatch_gen_rules", "dispatch_verify_rules"})
            system_prompt += GATE_PROMPT_SUFFIX

        # Tool cua job: KHONG viet cung o day. Job cong bo trong dong hello, ta dung NGUYEN
        # schema do. Mot pipeline khac mo them tool khac thi cho nay khong phai sua gi.
        if live_channel.available():
            try:
                announced = live_channel.handshake()
            except live_channel.LiveChannelError as e:
                print(f"CANH BAO: khong doc duoc cong bo tool cua job: {e}", file=sys.stderr)
                announced = []
            for t in announced:
                name = t.get("name")
                if not name:
                    continue
                tools.add_raw(name, t.get("description", ""),
                              t.get("parameters", {"type": "object", "properties": {}}),
                              _make_live_tool(name))
            if announced:
                system_prompt += _job_tools_prompt(announced)
                print(f"=== job cong bo {len(announced)} tool: "
                      f"{', '.join(t.get('name', '?') for t in announced)}", file=sys.stderr)
    else:
        tools = tools.subset({"checkpoint_decision", "request_human_decision"})
        system_prompt = SYSTEM_PROMPT
        stop_tools = {"report_to_user"}

    try:
        stop_tool, stop_result, transcript = run_tool_calling_agent(
            system_prompt=system_prompt,
            user_prompt=user_request,
            tools_schema=tools.schema(),
            tool_dispatch=tools.dispatch(),
            stop_tools=stop_tools,
        )
    except AgentLoopError as e:
        # RAISE chu khong sys.exit: SystemExit khong phai con cua Exception, nen
        # _run_checkpoint_cli se khong bat duoc va tien trinh chet TRUOC khi kip ghi decision
        # va ghi /dq/checkpoints/. Da xay ra that - job mat han dau vet checkpoint pre_rules.
        print(f"LOI: {e}", file=sys.stderr)
        raise

    if checkpoint:
        print(f"\n=== QUYET DINH CHECKPOINT '{checkpoint}' ===\n{stop_result}", file=sys.stderr)
    else:
        print(f"\n=== TRA LOI NGUOI DUNG ===\n{stop_result.get('message')}")
    return stop_result, transcript, state["attempts"]


CHECKPOINT_PROMPT_SUFFIX = """

=== CHE DO CHECKPOINT (dang duoc goi TU BEN TRONG job Spark) ===
Lan nay BAN KHONG noi chuyen voi nguoi dung. Job Spark dang CHAY VA DANG CHO ban tra loi —
moi giay ban suy nghi la mot giay cluster nam khong. Hay quyet dinh nhanh va dut khoat.

- KHONG goi report_to_user. Ket thuc bang checkpoint_decision(action, reason).
- action="PROCEED": job chay tiep binh thuong. Day la lua chon MAC DINH.
- action="HALT": CHI dung khi co bang chung ro rang rang chay tiep se sinh ra du lieu sai/hong.
  HALT la khuyen nghi duoc ghi lai va bao cao, hay neu reason that cu the.

Checkpoint "pre_rules": truoc khi job gan DQ check. Viec cua ban la dam bao co mot tap rule
active DUNG va DU TOT de job dung. Lam theo DUNG thu tu sau:

1. get_pipeline_context() de biet dang co gi.
2. Sinh lai rule khi VA CHI KHI mot trong bon dieu nay dung:
     active_rules.exists = false
     lineage_changed_since_rules = true
     checkpoint_coverage.needs_regeneration = true
     rules_verification.verified = false
   -> dispatch_gen_rules(). Gen Rules tra bo rule ve BO NHO, KHONG ghi xuong kho.
3. NEU (va CHI NEU) buoc 2 da sinh rule moi -> BAT BUOC dispatch_verify_rules().
   Day la khau tham dinh, va no la khau DUY NHAT: bo rule chi duoc ghi xuong kho sau khi
   verdict dat. Khong goi thi KHONG CO GI duoc luu va job se chay bang bo rule cu.
   Vi sao khong the bo qua: get_pipeline_context chi dem duoc SO COT duoc phu, no KHONG doc
   noi dung rule. Chi Verify Rules moi doi chieu tung rule voi manifest + lineage va bat duoc
   loai loi nhu "rule IS NULL cho cot end_province la rule chet, vi lineage cho thay cot do
   la coalesce(city_code,'unknown') nen khong bao gio NULL".
   - Neu tra ve status="rejected": doc must_fix roi dispatch_gen_rules(feedback="<dung nhung
     gi verdict yeu cau sua>") va dispatch_verify_rules() lai. Co gioi han cung so vong.
   - Neu van khong dat sau khi het luot regen: van checkpoint_decision("PROCEED", ...) nhung
     NEU RO trong reason la tap rule chua dat, de nguoi doc report biet ket qua DQ lan nay yeu.
4. Neu buoc 2 KHONG sinh rule moi -> KHONG tham dinh lai. Bo rule do da co verdict tu lan sinh
   truoc; tham dinh lai chi ton them mot vong LLM trong khi job dang nam cho.
5. checkpoint_decision("PROCEED", ...) de job doc rule va chay tiep. Viec THUC THI rule la
   cua job Spark (DqMetricQuery), khong phai cua agent - ban khong co tool nao lam duoc.

Checkpoint "post_report": report DQ vua duoc ghi xong. Goi dispatch_analysis de phan tich
root-cause/predictive, roi checkpoint_decision voi khuyen nghi tuong ung.
"""

def _job_tools_prompt(announced: list) -> str:
    """Khoi prompt mo ta cac tool JOB cong bo o checkpoint nay.

    Noi dung tung tool do chinh job viet (DqTool.description phia Java), khong phai o day.
    Job biet ro nhat no cho phep lam gi va khi nao nen dung - giu mo ta o day nghia la moi lan
    job doi kha nang thi phai nho sua ca file nay, va hai ban se lech.
    """
    lines = [
        "",
        "",
        "=== TOOL DO JOB SPARK MO RA O CHECKPOINT NAY ===",
        "Job dang CHAY va dang giu du lieu trong bo nho. Nhung tool duoi day chi dung duoc",
        "ngay bay gio; het checkpoint la khong con.",
        "",
    ]
    for t in announced:
        lines.append(f"- {t.get('name')}: {t.get('description', '')}")
    lines += [
        "",
        "DAT TIEN: moi lan goi la mot lan job that su lam viec, va job dang nam cho ban.",
        "Chi goi khi thong tin san co KHONG du de ket luan.",
        "",
    ]
    return "\n".join(lines)


def _run_checkpoint_cli(checkpoint: str, context_file: str, decision_out: str) -> int:
    """Entry point DqCheckpoint.java goi. Luon ghi decision, ke ca khi that bai.

    Mac dinh moi duong that bai deu la PROCEED: DQ khong bao gio duoc lam hong pipeline.
    """
    import json

    decision = {"action": "PROCEED", "reason": "agent khong tra ve quyet dinh nao"}
    try:
        with open(context_file, "r", encoding="utf-8") as f:
            context = json.load(f)
        prompt = (
            f"Checkpoint: {checkpoint}\n"
            f"Job Spark dang chay va dang CHO quyet dinh cua ban.\n"
            f"Du lieu job gui sang:\n{json.dumps(context, indent=2, ensure_ascii=False)}\n\n"
            "Hay xu ly checkpoint nay roi ket thuc bang checkpoint_decision(action, reason)."
        )
        stop_result, transcript, attempts = run(prompt, checkpoint=checkpoint,
                                                job_context=context)
        if isinstance(stop_result, dict) and stop_result.get("action"):
            decision = {"action": stop_result["action"], "reason": stop_result.get("reason", "")}
    except BaseException as e:  # KE CA SystemExit/KeyboardInterrupt - xem ghi chu o run()
        decision = {"action": "PROCEED",
                    "reason": f"checkpoint loi ({type(e).__name__}), cho job chay tiep: {e}"}
        print(f"LOI checkpoint: {type(e).__name__}: {e}", file=sys.stderr)

    with open(decision_out, "w", encoding="utf-8") as f:
        json.dump(decision, f, ensure_ascii=False)
    print(f"=== decision -> {decision_out}: {decision}", file=sys.stderr)

    # Ghi lai quyet dinh len /dq/checkpoints/. Khong co buoc nay thi bang chung agent da chay
    # chi nam trong log driver pod - ma log do bi xoay mat khi pod bi don, da gap that.
    try:
        ctx = locals().get("context") or {}
        run_id = ctx.get("spark_app_id") or "unknown-run"
        import live_channel as _lc
        dq_store_write.record_checkpoint(
            ctx.get("dq_key") or DQ_KEY, run_id, checkpoint,
            {**decision, "recorded_at": datetime.now(timezone.utc).isoformat(),
             "live_channel": _lc.available()},
            transcript=locals().get("transcript"),
            attempts=locals().get("attempts"))
        print(f"=== da ghi checkpoint len /dq/checkpoints/{ctx.get('dq_key') or DQ_KEY}/{run_id}.json", file=sys.stderr)
    except Exception as e:  # ghi nhat ky that bai khong duoc lam hong job
        print(f"CANH BAO: khong ghi duoc checkpoint len HDFS: {e}", file=sys.stderr)

    # Dong CUOI CUNG tren stdout: bao cho job Java biet agent da xong va quyet dinh gi.
    import live_channel
    if live_channel.available():
        live_channel.send_decision(decision["action"], decision.get("reason", ""))
    return 0


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("request", nargs="?",
                        default=f"Kiem tra chat luong du lieu cho {DQ_KEY}.")
    parser.add_argument("--checkpoint", help="Ten checkpoint khi duoc goi tu job Spark.")
    parser.add_argument("--context-file", help="File JSON chua du lieu job gui sang.")
    parser.add_argument("--decision-out", help="File JSON de ghi quyet dinh tra ve cho job.")
    # Che tra loi quyet dinh cua NGUOI. Khong goi LLM - chi ghi mot file. Van chay qua
    # orchestrator chu khong de dich vu HTTP tu ghi, vi quyen ghi vao /dq/ la cua rieng
    # orchestrator (dq_store_write tu choi import khi DQ_ROLE khac).
    parser.add_argument("--answer-decision", action="store_true",
                        help="Ghi cau tra loi cua nguoi cho mot checkpoint dang cho.")
    parser.add_argument("--run-id", help="Chi dung voi --answer-decision.")
    parser.add_argument("--stage", help="Chi dung voi --answer-decision.")
    parser.add_argument("--action", help="Chi dung voi --answer-decision.")
    parser.add_argument("--decided-by", help="Ai quyet dinh. Chi dung voi --answer-decision.")
    parser.add_argument("--reason", help="Ly do. Chi dung voi --answer-decision.")
    args = parser.parse_args()

    if args.answer_decision:
        missing = [n for n in ("run_id", "stage", "action", "decided_by", "reason")
                   if not getattr(args, n)]
        if missing:
            parser.error("--answer-decision can: " + ", ".join("--" + m.replace("_", "-")
                                                               for m in missing))
        act = args.action.strip().upper()
        if act not in _CHECKPOINT_ACTIONS or act == _AWAIT_HUMAN:
            parser.error(f"action khong hop le: {act}. Nguoi chon mot trong: "
                         + ", ".join(a for a in _CHECKPOINT_ACTIONS if a != _AWAIT_HUMAN))
        try:
            answered = dq_store_write.answer_decision(
                DQ_KEY, args.run_id, args.stage, act, args.decided_by, args.reason)
        except Exception as e:
            print(json.dumps({"ok": False, "error": str(e)}, ensure_ascii=False))
            sys.exit(1)
        print(json.dumps({"ok": True, "decision": answered}, ensure_ascii=False))
        sys.exit(0)

    if args.checkpoint:
        if not args.context_file or not args.decision_out:
            parser.error("--checkpoint can ca --context-file va --decision-out")
        sys.exit(_run_checkpoint_cli(args.checkpoint, args.context_file, args.decision_out))

    try:
        run(args.request)
    except AgentLoopError:
        sys.exit(1)
