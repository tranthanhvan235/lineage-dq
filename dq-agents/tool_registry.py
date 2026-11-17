"""Khai bao tool cua agent mot lan: schema va ham thuc thi di cung nhau.

Truoc day schema (gui cho LLM) va dispatch (ham chay that) la hai thu roi nhau, khong gi bat
chung khop. O day mot khai bao sinh ra ca hai, schema tham so doc tu CHU KY HAM:

    tools = ToolRegistry()

    @tools.tool("Lay ban tom tat ngu canh pipeline. Nhanh, khong ton LLM.")
    def get_pipeline_context():
        ...

    @tools.tool("Sinh/regen bo rule DQ.",
                param_docs={"feedback": "Phan hoi neu day la regen."})
    def dispatch_gen_rules(feedback: str = None):
        ...

    run_tool_calling_agent(tools_schema=tools.schema(), tool_dispatch=tools.dispatch(), ...)

Tham so KHONG co gia tri mac dinh la bat buoc; co mac dinh la tuy chon. Kieu lay tu annotation.
"""
import inspect

_TYPES = {str: "string", int: "integer", float: "number", bool: "boolean",
         list: "array", dict: "object"}


class ToolRegistry:

    def __init__(self):
        self._schema = []
        self._dispatch = {}

    def tool(self, description: str, name: str = None, param_docs: dict = None,
             param_schema: dict = None):
        """Decorator dang ky mot tool.

        name         : mac dinh la ten ham
        param_docs   : {ten tham so: mo ta} - chen vao schema sinh tu chu ky ham
        param_schema : ghi de hoan toan schema mot tham so, vd
                       {"analysis_type": {"type": "string", "enum": ["ROOT_CAUSE", ...]}}
        """
        def _register(fn):
            tool_name = name or fn.__name__
            self._schema.append({"type": "function", "function": {
                "name": tool_name,
                "description": description,
                "parameters": _schema_from_signature(fn, param_docs or {}, param_schema or {}),
            }})
            self._dispatch[tool_name] = fn
            return fn
        return _register

    def add_raw(self, name: str, description: str, parameters: dict, fn):
        """Dang ky tool ma schema den tu noi khac (job Spark cong bo qua kenh live).

        Lay nguyen schema ben kia gui, khong dien giai lai - dien giai lai la tao ban sao thu hai.
        """
        self._schema.append({"type": "function", "function": {
            "name": name, "description": description, "parameters": parameters,
        }})
        self._dispatch[name] = fn

    def schema(self) -> list:
        return list(self._schema)

    def dispatch(self) -> dict:
        return dict(self._dispatch)

    def subset(self, exclude: set = None):
        """Ban sao khong chua cac tool nay - vd che do checkpoint khong duoc lo report_to_user."""
        exclude = exclude or set()
        new = ToolRegistry()
        new._schema = [s for s in self._schema if s["function"]["name"] not in exclude]
        new._dispatch = {k: v for k, v in self._dispatch.items() if k not in exclude}
        return new

def _schema_from_signature(fn, param_docs: dict, overrides: dict) -> dict:
    props = {}
    required = []
    for name, p in inspect.signature(fn).parameters.items():
        if name in overrides:
            props[name] = dict(overrides[name])
        else:
            props[name] = {"type": _TYPES.get(p.annotation, "string")}
        if name in param_docs:
            props[name]["description"] = param_docs[name]
        if p.default is inspect.Parameter.empty:
            required.append(name)
    return {"type": "object", "properties": props, "required": required}
