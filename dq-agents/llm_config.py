import os

# Gia tri that nam o dq_env.py - ha tang dung chung cho moi pipeline.
from dq_env import (
    LLM_API_KEY, LLM_BASE_URL, LLM_MODEL, LLM_MAX_ITERATIONS,
    LLM_REQUEST_TIMEOUT_SEC, LLM_MAX_RETRIES)


def require_api_key() -> str:
    if not LLM_API_KEY:
        raise RuntimeError("Missing LLM_API_KEY or GROQ_API_KEY environment variable.")
    return LLM_API_KEY
