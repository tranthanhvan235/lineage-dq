"""Standard envelope orchestrator.py wraps each subagent's raw result in before handing it to
the tool-calling loop, so the LLM reads {ok, status} uniformly instead of a differently-shaped
outcome/action/error key per subagent. Wraps, does not replace -- each agent's own result dict
is preserved under "detail".
"""


def SubagentResult(ok: bool, status: str, summary: str, detail: dict = None) -> dict:
    return {"ok": ok, "status": status, "summary": summary, "detail": detail or {}}
