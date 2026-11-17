"""Client Trino toi thieu, chi dung thu vien chuan.

O DAY vi day la thu muc python dung chung cua framework: dashboard/ import tu day, va DAG Airflow
cung nhan chinh thu muc nay qua ConfigMap. Dat o mot trong hai cho kia thi cho con lai phai
chep - dung thu vua bi cam.

Khong dung goi 'trino' tren PyPI: mot lenh POST va mot vong lap theo nextUri la du, va them
mot dependency vao image agent chi de doc bang la khong dang.
"""
import base64
import json
import os
import ssl
import urllib.error
import urllib.request

TRINO_URL = os.getenv("TRINO_URL", "http://localhost:30808")
TRINO_USER = os.getenv("TRINO_USER", "dq")
CATALOG = os.getenv("TRINO_CATALOG", "iceberg")
TIMEOUT = int(os.getenv("TRINO_TIMEOUT_SEC", "60"))
# Xac thuc mat khau (LDAP/file) cua Trino. Bo trong = khong xac thuc, dung nhu cum MicroK8s.
# Trino chi nhan mat khau qua HTTPS, nen co mat khau thi TRINO_URL phai la https://.
TRINO_PASSWORD = os.getenv("TRINO_PASSWORD", "")
# CA noi bo khi chung chi cua Trino khong do CA cong khai ky.
TRINO_CA_FILE = os.getenv("TRINO_CA_FILE", "")

_HEADERS = {"X-Trino-User": TRINO_USER, "Content-Type": "text/plain"}
if TRINO_PASSWORD:
    _HEADERS["Authorization"] = "Basic " + base64.b64encode(
        f"{TRINO_USER}:{TRINO_PASSWORD}".encode("utf-8")).decode("ascii")
_SSL_CONTEXT = ssl.create_default_context(cafile=TRINO_CA_FILE) if TRINO_CA_FILE else None


class TrinoError(RuntimeError):
    pass


class TrinoUnreachable(TrinoError):
    """Khong noi duoc toi Trino - KHAC HAN voi Trino tu choi cau lenh.

    Co lop rieng vi dashboard phai phan biet hai ca nay: khong noi duoc la 503, cau lenh sai
    la 400. Truoc day no phan biet bang message.startswith("khong ket noi duoc Trino") - mot
    rang buoc vo hinh giua hai file, va sua chinh ta cau thong bao la hong phan loai ma khong
    ai biet. Van ke thua TrinoError nen moi 'except TrinoError' dang co van bat duoc."""


def _fetch(url, data=None):
    req = urllib.request.Request(
        url,
        data=data.encode() if data else None,
        headers=_HEADERS,
    )
    with urllib.request.urlopen(req, timeout=TIMEOUT, context=_SSL_CONTEXT) as r:
        return json.load(r)


def query(sql: str):
    """Chay mot cau SQL, tra ve (ten_cot, cac_dong).

    Trino tra ket qua theo NHIEU TRANG: lan dau chi la bien nhan trang thai QUEUED, du lieu
    den o cac trang sau qua nextUri. Dung o trang dau la mat du lieu ma khong he bao loi -
    con loi SQL thi nguoc lai, chi xuat hien o trang cuoi. Phai di het.
    """
    try:
        doc = _fetch(f"{TRINO_URL}/v1/statement", data=sql)
    except urllib.error.URLError as e:
        raise TrinoUnreachable(f"Không nối được Trino tại {TRINO_URL}: {e}") from e

    columns, rows = [], []
    while True:
        if doc.get("error"):
            e = doc["error"]
            raise TrinoError(f"{e.get('errorName')}: {e.get('message')}")
        if doc.get("columns") and not columns:
            columns = [c["name"] for c in doc["columns"]]
        rows.extend(doc.get("data") or [])
        nxt = doc.get("nextUri")
        if not nxt:
            return columns, rows
        doc = _fetch(nxt)


def df(sql: str):
    """Tra ve pandas DataFrame. Chi goi duoc o noi co pandas (demo), khong goi trong agent."""
    import pandas as pd
    columns, rows = query(sql)
    return pd.DataFrame(rows, columns=columns or None)


def is_alive() -> bool:
    try:
        query("SELECT 1")
        return True
    except Exception:
        return False


def schemas_with_table(table_name: str) -> list:
    """Cac schema thuc su chua bang do.

    Bang DQ nam trong DATABASE DAU RA cua tung pipeline chu khong o mot cho dung chung:
    DqGuard nhan warehouse(outDb), nen warehouse_a giu bo bang cua no va warehouse_b giu bo rieng.
    Them mot pipeline ghi ra database moi thi o day tu moc them mot schema - khong phai sua code.
    """
    _, rows = query(
        f"SELECT table_schema FROM {CATALOG}.information_schema.tables "
        f"WHERE table_name = '{table_name}' ORDER BY 1"
    )
    return [d[0] for d in rows]


def columns_of(schema: str, table_name: str) -> list:
    """[(ten_cot, kieu)] theo dung thu tu khai bao."""
    _, rows = query(
        f"SELECT column_name, data_type FROM {CATALOG}.information_schema.columns "
        f"WHERE table_schema = '{schema}' AND table_name = '{table_name}' "
        f"ORDER BY ordinal_position"
    )
    return [(d[0], d[1]) for d in rows]


def union_all_schemas(table_name: str, columns: str = "*") -> str:
    """UNION ALL qua moi schema co bang do, kem cot source_schema de con biet dong tu dau.

    Gop theo TEN COT chu khong theo vi tri. Moi pipeline giu bo bang trong database dau ra cua
    rieng no, nen chung tien hoa doc lap: them mot cot cho pipeline nay ma chua vá pipeline kia
    la `SELECT *` vo ngay ("UNION query has different number of fields"), hoac te hon la ghep
    dung so cot nhung SAI cot voi nhau. Cot thieu o mot schema duoc dien NULL dung kieu.

    columns="*" moi gop theo ten; truyen danh sach cot cu the thi giu nguyen nhu cu.
    """
    schemas = schemas_with_table(table_name)
    if not schemas:
        raise TrinoError(f"khong schema nao trong {CATALOG} co bang {table_name}")
    if columns != "*":
        return " UNION ALL ".join(
            f"SELECT '{s}' AS source_schema, {columns} FROM {CATALOG}.{s}.{table_name}"
            for s in schemas)

    by_schema = {s: dict(columns_of(s, table_name)) for s in schemas}
    # Thu tu chot theo schema DAU TIEN, roi noi them cot chi co o schema sau - de ket qua on
    # dinh giua cac lan goi chu khong doi theo schema nao tinh co nhieu cot hon.
    ordered, types = [], {}
    for s in schemas:
        for name, typ in columns_of(s, table_name):
            if name not in types:
                ordered.append(name)
                types[name] = typ

    parts = []
    for s in schemas:
        have = by_schema[s]
        cols = ", ".join(
            name if name in have else f"CAST(NULL AS {types[name]}) AS {name}"
            for name in ordered)
        parts.append(f"SELECT '{s}' AS source_schema, {cols} FROM {CATALOG}.{s}.{table_name}")
    return " UNION ALL ".join(parts)
