"""Vòng lặp tool-calling dùng chung cho mọi agent.

LangChain chỉ lo chuẩn hoá message và parse tool_calls; vòng lặp điều phối vẫn ở đây, KHÔNG
dùng AgentExecutor prebuilt — ta cần ngữ nghĩa "stop-tool kết thúc ngay", transcript audit, và
tool được phép sys.exit, những thứ AgentExecutor không lộ ra.
"""
import os
import sys
import json

from langchain_openai import ChatOpenAI
from langchain_core.messages import (
    SystemMessage,
    HumanMessage,
    AIMessage,
    ToolMessage,
)
from llm_config import (LLM_BASE_URL, LLM_MAX_ITERATIONS, LLM_MAX_RETRIES, LLM_MODEL,
                        LLM_REQUEST_TIMEOUT_SEC, require_api_key)


class AgentLoopError(Exception):
    """Agent không kết thúc đúng cách (vd chạm max_iterations mà chưa gọi tool kết thúc)."""


def run_tool_calling_agent(
    system_prompt: str,
    user_prompt: str,
    tools_schema: list,
    tool_dispatch: dict,
    stop_tools: set,
    max_iterations: int = LLM_MAX_ITERATIONS,
    model: str = LLM_MODEL,
):
    """LLM tự quyết định gọi tool nào, thứ tự nào.

    - tools_schema:  định dạng OpenAI, bind_tools() nhận trực tiếp
    - tool_dispatch: {tên: callable(**kwargs) -> dict}
    - stop_tools:    gọi thành công một trong số này là kết thúc vòng lặp
    - max_iterations: lưới an toàn khi model không bao giờ gọi tool kết thúc

    Trả về (stop_tool_name, stop_tool_result, transcript).
    """
    api_key = require_api_key()

    # timeout + max_retries: khong dat thi mot lan goi treo se giu tien trinh mai mai.
    llm = ChatOpenAI(model=model, api_key=api_key, base_url=LLM_BASE_URL,
                     timeout=LLM_REQUEST_TIMEOUT_SEC, max_retries=LLM_MAX_RETRIES)
    llm_with_tools = llm.bind_tools(tools_schema)

    messages = [
        SystemMessage(content=system_prompt),
        HumanMessage(content=user_prompt),
    ]
    transcript = []

    for _iteration in range(1, max_iterations + 1):
        ai_msg: AIMessage = llm_with_tools.invoke(messages)
        # Phải append AIMessage (kèm tool_calls) để ToolMessage trả về sau khớp được với nó.
        messages.append(ai_msg)

        if not ai_msg.tool_calls:
            # Model trả lời tự do, chưa gọi tool nào — chưa kết thúc hợp lệ, nhắc lại.
            messages.append(HumanMessage(content=(
                "Bạn PHẢI gọi một trong các tool để tiếp tục hoặc kết thúc. "
                "Không được chỉ trả lời bằng văn bản tự do."
            )))
            continue

        for tc in ai_msg.tool_calls:
            tool_name = tc["name"]
            arguments = tc.get("args", {}) or {}

            handler = tool_dispatch.get(tool_name)
            if handler is None:
                result = {"error": f"Không tồn tại tool tên '{tool_name}'."}
            else:
                try:
                    result = handler(**arguments)
                except Exception as e:  # báo lỗi tool về cho model tự xử lý
                    result = {"error": f"Lỗi khi thực thi tool '{tool_name}': {e}"}

            transcript.append({
                "sequence_no": len(transcript) + 1,
                "tool_name": tool_name,
                "arguments": arguments,
                "result": result,
            })
            # stderr, KHÔNG phải stdout: ở chế độ checkpoint stdout là kênh giao thức với job.
            print(f"  [{len(transcript)}] {tool_name}({arguments}) -> {result}",
                  file=sys.stderr, flush=True)
            messages.append(ToolMessage(
                content=json.dumps(result, ensure_ascii=False, default=str),
                tool_call_id=tc["id"],
            ))

            # Stop-tool thành công (không có "error") -> kết thúc ngay, không invoke model nữa.
            if tool_name in stop_tools and not (isinstance(result, dict) and result.get("error")):
                return tool_name, result, transcript

    raise AgentLoopError(
        f"Agent không gọi tool kết thúc ({stop_tools}) sau {max_iterations} vòng lặp."
    )
