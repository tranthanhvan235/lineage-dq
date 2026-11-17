"""
Lightweight WebHDFS client for reading Iceberg table partition layout -- no Spark/Hive needed.

Used to ground-truth "which partition (date/hour) did a given job run actually write to",
since Marquez's run history (lineage_client.get_row_count_history) doesn't carry the job's
PROCESS_DATE argument -- Marquez's `args` field on a run is parent-run linkage metadata
(run_id/name/namespace), not the actual spark-submit CLI arguments.

Deliberately NOT used to recompute row counts: Iceberg's dynamic partition overwrite keeps
superseded data files physically in place (for snapshot/time-travel) until an expire-snapshots
compaction runs -- a raw directory listing's file count/byte size double-counts stale files. A
real table with 5 overwrites of the same partition still has 5 files sitting in that partition's
folder even though only the latest snapshot's file is "active" data. Only Iceberg's own manifest
(or, more simply, OpenLineage's per-run outputStatistics already surfaced via Marquez) reflects
the CURRENTLY ACTIVE row count. This module is for partition *identity* (which date), not counts.
"""
import json
import os
import sys
import time
import urllib.error
import urllib.request

from dq_env import WEBHDFS_URL, HDFS_WEBHDFS_USER  # giu ten cu cho code goi


# ── Kho dq_store doc qua Trino (dashboard, CHI DOC) ─────────────────────
# Cum HDFS bat Kerberos va job chay DQ_STORE=iceberg: tai lieu DQ khong con la file duoi /dq ma la
# dong trong bang <catalog>.<db>.dq_store (path = duong dan cu, ban moi nhat = version lon nhat).
# DQ_STORE_TRINO=1 -> doc/liet ke qua Trino, tu tim moi schema co bang dq_store.
# DQ_STORE_TRINO_TABLES=a.b.dq_store,... -> chi doc dung cac bang nay (cung tu bat).
_TRINO_TABLES_ENV = [t.strip() for t in os.getenv("DQ_STORE_TRINO_TABLES", "").split(",") if t.strip()]
_TRINO_ON = (os.getenv("DQ_STORE_TRINO", "").strip().lower() in ("1", "true", "yes")
             or bool(_TRINO_TABLES_ENV))
_TRINO_TABLES_TTL_SEC = 600          # tu tim lai bang dq_store moi 10 phut (them pipeline/db moi)
_trino_tables_cache = None           # (danh sach bang, thoi diem tim - monotonic)


def trino_store_on() -> bool:
    return _TRINO_ON


def trino_store_tables() -> list:
    """Cac bang dq_store se doc. [] khi chua co bang nao. Nem loi khi Trino khong tra loi."""
    global _trino_tables_cache
    if _TRINO_TABLES_ENV:
        return list(_TRINO_TABLES_ENV)
    now = time.monotonic()
    if _trino_tables_cache is None or now - _trino_tables_cache[1] > _TRINO_TABLES_TTL_SEC:
        import trino_client
        found = [f"{trino_client.CATALOG}.{s}.dq_store"
                 for s in trino_client.schemas_with_table("dq_store")]
        _trino_tables_cache = (found, now)
    return list(_trino_tables_cache[0])


# Ghi qua Trino: TACH RIENG khoi doc. Chi tien trinh can ghi (dq-agent-api, de tra loi Decisions)
# bat co nay; backend dashboard KHONG bat, nen van chi doc du dung chung module nay.
_TRINO_WRITE_ON = os.getenv("DQ_STORE_TRINO_WRITE", "").strip().lower() in ("1", "true", "yes")
# Bang nhan ban ghi moi. Bo trong thi dung bang duy nhat tim thay; tim thay nhieu bang ma khong
# chi dinh thi TU CHOI ghi - doan bang la ghi quyet dinh vao noi job khong doc.
_TRINO_WRITE_TABLE = os.getenv("DQ_STORE_TRINO_WRITE_TABLE", "").strip()
_trino_write_cols = {}               # bang -> {ten cot: kieu}, doc mot lan


def _trino_write_target() -> str:
    if _TRINO_WRITE_TABLE:
        return _TRINO_WRITE_TABLE
    tables = trino_store_tables()
    if len(tables) == 1:
        return tables[0]
    raise RuntimeError(
        f"Khong biet ghi vao bang dq_store nao (tim thay {tables or 'khong co bang nao'}). "
        "Dat DQ_STORE_TRINO_WRITE_TABLE=<catalog>.<db>.dq_store.")


def _trino_columns(table: str) -> dict:
    if table not in _trino_write_cols:
        import trino_client
        catalog, schema, name = table.split(".")
        _, rows = trino_client.query(
            f"SELECT column_name, data_type FROM {catalog}.information_schema.columns "
            f"WHERE table_schema = {_sql_lit(schema)} AND table_name = {_sql_lit(name)}")
        _trino_write_cols[table] = {r[0]: r[1] for r in rows}
    return _trino_write_cols[table]


def _trino_write(path: str, text: str) -> None:
    """Them MOT dong moi cho `path` voi version lon hon moi dong cu - ban moi nhat thang khi doc
    (xem _trino_read). Khong UPDATE/DELETE: lich su con nguyen, va Iceberg ghi them re hon sua."""
    import trino_client
    table = _trino_write_target()
    cols = _trino_columns(table)
    missing = [c for c in ("path", "content", "version") if c not in cols]
    if missing:
        raise RuntimeError(f"Bang {table} thieu cot {missing} - khong phai bang dq_store?")
    names = ["path", "content", "version"]
    values = [_sql_lit(path), _sql_lit(text), str(int(time.time() * 1_000_000))]
    if "written_at" in cols:
        # Ep dung kieu cua cot (timestamp(6), timestamp(3) with time zone...): Trino khong tu
        # ep timestamp co mui gio sang khong mui gio khi INSERT.
        names.append("written_at")
        values.append(f"CAST(current_timestamp AS {cols['written_at']})")
    trino_client.query(f"INSERT INTO {table} ({', '.join(names)}) VALUES ({', '.join(values)})")


def trino_store_tables_known() -> list:
    """Nhu trino_store_tables() nhung KHONG BAO GIO goi Trino: chi tra ban da biet (bien moi
    truong, hoac lan tim gan nhat). [] khi chua tim lan nao. Dung cho duong phai nhanh va khong
    duoc treo, vd readinessProbe."""
    if _TRINO_TABLES_ENV:
        return list(_TRINO_TABLES_ENV)
    return list(_trino_tables_cache[0]) if _trino_tables_cache else []


def _sql_lit(s: str) -> str:
    return "'" + s.replace("'", "''") + "'"


def _trino_union(cols: str):
    tables = trino_store_tables()
    if not tables:
        return None
    return " UNION ALL ".join(f"SELECT {cols} FROM {t}" for t in tables)


def _trino_read(path: str):
    import trino_client
    src = _trino_union("path, content, version")
    if src is None:
        return None
    _, rows = trino_client.query(
        f"SELECT content FROM ({src}) WHERE path = {_sql_lit(path)} "
        f"ORDER BY version DESC LIMIT 1")
    return rows[0][0] if rows else None


def _trino_list(dir_path: str) -> list:
    """Muc truc tiep duoi mot "thu muc", cung shape FileStatus cua WebHDFS LISTSTATUS."""
    import trino_client
    src = _trino_union("path, version")
    if src is None:
        return []
    prefix = dir_path.rstrip("/") + "/"
    # So khop tien to bang substr, KHONG dung LIKE: '_' trong ten pipeline la ky tu dai dien cua LIKE.
    _, rows = trino_client.query(
        f"SELECT path, max(version) FROM ({src}) "
        f"WHERE substr(path, 1, {len(prefix)}) = {_sql_lit(prefix)} GROUP BY path")
    by_name = {}
    for path, version in rows:
        rest = path[len(prefix):]
        name, sep, _ = rest.partition("/")
        if not name:
            continue
        mtime = int(version) // 1000           # version = micro giay -> modificationTime mili giay
        e = by_name.setdefault(name, {"pathSuffix": name, "type": "DIRECTORY" if sep else "FILE",
                                      "modificationTime": mtime})
        e["modificationTime"] = max(e["modificationTime"], mtime)
    return list(by_name.values())


# ── Kho DQ tren file local (chay thu tren mot may, khong can HDFS) ──────
# DQ_STORE_LOCAL=/duong/dan -> moi duong dan /dq/... nam o /duong/dan/dq/...: dung cho do
# DqGuard ghi khi chay voi hdfsBaseUri=file:///duong/dan. Uu tien hon Trino va WebHDFS.
_LOCAL_BASE = os.getenv("DQ_STORE_LOCAL", "").strip()


def local_store_on() -> bool:
    return bool(_LOCAL_BASE)


def _local_path(path: str) -> str:
    return os.path.join(_LOCAL_BASE, path.lstrip("/"))


def _local_list(path: str) -> list:
    d = _local_path(path)
    if not os.path.isdir(d):
        return []
    out = []
    for name in sorted(os.listdir(d)):
        if name.startswith("."):           # .<file>.crc cua Hadoop LocalFileSystem
            continue
        full = os.path.join(d, name)
        st = os.stat(full)
        out.append({"pathSuffix": name, "type": "DIRECTORY" if os.path.isdir(full) else "FILE",
                    "length": st.st_size, "modificationTime": int(st.st_mtime * 1000)})
    return out


def _local_write(path: str, payload: bytes, overwrite: bool) -> None:
    f = _local_path(path)
    if os.path.exists(f) and not overwrite:
        raise FileExistsError(f)
    os.makedirs(os.path.dirname(f), exist_ok=True)
    tmp = f + ".tmp"
    with open(tmp, "wb") as fh:
        fh.write(payload)
    os.replace(tmp, f)
    # Hadoop doi chieu file voi .<ten>.crc khi doc; crc cu cua ban truoc lam Java doc loi.
    crc = os.path.join(os.path.dirname(f), f".{os.path.basename(f)}.crc")
    if os.path.exists(crc):
        os.unlink(crc)


def _local_read(path: str):
    try:
        with open(_local_path(path), encoding="utf-8") as fh:
            return fh.read()
    except FileNotFoundError:
        return None


def _liststatus(hdfs_path: str) -> list:
    if local_store_on():
        return _local_list(hdfs_path)
    if trino_store_on():
        try:
            return _trino_list(hdfs_path)
        except Exception as e:
            print(f"WARNING: dq_store (Trino) list that bai cho {hdfs_path}: {e}", file=sys.stderr)
            return []
    url = f"{WEBHDFS_URL}/webhdfs/v1{hdfs_path}?op=LISTSTATUS&user.name={HDFS_WEBHDFS_USER}"
    try:
        req = urllib.request.Request(url, method="GET")
        with urllib.request.urlopen(req, timeout=10) as response:
            if response.status != 200:
                print(f"WARNING: WebHDFS LISTSTATUS returned status {response.status} for {hdfs_path}")
                return []
            data = json.loads(response.read().decode("utf-8"))
        return data.get("FileStatuses", {}).get("FileStatus", [])
    except (urllib.error.HTTPError, urllib.error.URLError) as e:
        print(f"WARNING: WebHDFS LISTSTATUS failed for {hdfs_path}: {e}")
        return []


def list_dir(hdfs_path: str) -> list:
    """Ten cac muc trong mot thu muc HDFS. [] khi thu muc khong ton tai hoac doc that bai.

    Cong khai hoa _liststatus: lich su so dong cua framework nam o /dq/reports/{key}/history/
    va can liet ke duoc. Truoc day chi lay duoc qua Marquez REST, ma Marquez la lop hien thi
    chu khong phai nguon du lieu cua core.
    """
    return [entry.get("pathSuffix") for entry in _liststatus(hdfs_path)
            if entry.get("pathSuffix")]


def list_partition_values(table_data_path: str, partition_col: str) -> list:
    """List partition values for a Hive-style-partitioned Iceberg table's data directory.

    table_data_path example: "/warehouse/<db>.db/<table>/data"
    partition_col example: "date_time" (folders named "date_time=<value>")

    Returns [{"value": "2026-08-27", "latest_mtime_ms": 1788...}], newest-first by
    latest_mtime_ms -- the max file mtime found inside that partition's folder, i.e. the most
    recent moment ANY write touched it (including superseded files, since we only need the
    max here, not a count). Used purely to correlate with a Marquez run's endedAt timestamp.
    """
    prefix = f"{partition_col}="
    entries = _liststatus(table_data_path)
    partitions = []
    for e in entries:
        name = e.get("pathSuffix", "")
        if e.get("type") != "DIRECTORY" or not name.startswith(prefix):
            continue
        value = name[len(prefix):]
        files = _liststatus(f"{table_data_path}/{name}")
        mtimes = [f["modificationTime"] for f in files if f.get("type") == "FILE"]
        latest_mtime_ms = max(mtimes) if mtimes else e.get("modificationTime", 0)
        partitions.append({"value": value, "latest_mtime_ms": latest_mtime_ms})

    partitions.sort(key=lambda p: p["latest_mtime_ms"], reverse=True)
    return partitions


def _resolve_datanode_pod_ip():
    """The WebHDFS CREATE redirect points at the datanode's in-cluster k8s DNS hostname,
    unresolvable from this host -- substitute the pod's ClusterIP-reachable IP instead. Same
    manual recipe used earlier this session for uploading the job jar via WebHDFS.
    """
    import subprocess
    hostname = subprocess.run(
        ["microk8s", "kubectl", "get", "pod", "-n", "hdfs", "-l", "app=hdfs-datanode",
         "-o", "jsonpath={.items[0].metadata.name}"],
        capture_output=True, text=True, timeout=15,
    ).stdout.strip()
    pod_ip = subprocess.run(
        ["microk8s", "kubectl", "get", "pod", "-n", "hdfs", "-l", "app=hdfs-datanode",
         "-o", "jsonpath={.items[0].status.podIP}"],
        capture_output=True, text=True, timeout=15,
    ).stdout.strip()
    if not hostname or not pod_ip:
        raise RuntimeError("Could not resolve hdfs-datanode pod name/IP via kubectl.")
    return hostname, pod_ip


from dq_env import HDFS_NAMESPACE, HDFS_DATANODE_SELECTOR, HDFS_DATANODE_REWRITE
def _resolve_datanode_via_k8s_api():
    """Same lookup as _resolve_datanode_pod_ip, but through the Kubernetes API.

    Needed because the agents now also run INSIDE the Spark driver pod, where there is no
    microk8s/kubectl binary -- but the pod's service account can query the API directly.
    Returns None when not running in a pod (no service-account token mounted).
    """
    import ssl
    import urllib.parse

    token_path = "/var/run/secrets/kubernetes.io/serviceaccount/token"
    ca_path = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"
    if not os.path.exists(token_path):
        return None

    with open(token_path, "r", encoding="utf-8") as f:
        token = f.read().strip()
    url = (f"https://kubernetes.default.svc/api/v1/namespaces/{HDFS_NAMESPACE}/pods"
           f"?labelSelector={urllib.parse.quote(HDFS_DATANODE_SELECTOR)}")
    req = urllib.request.Request(url, headers={"Authorization": f"Bearer {token}"})
    with urllib.request.urlopen(req, timeout=10, context=ssl.create_default_context(cafile=ca_path)) as r:
        items = json.loads(r.read().decode("utf-8")).get("items") or []
    for pod in items:
        name = pod.get("metadata", {}).get("name")
        ip = pod.get("status", {}).get("podIP")
        if name and ip:
            return name, ip
    return None


# Truoc day _redirect_target phan giai lai tu dau cho MOI lan doc/ghi MOT file, va moi lan la
# HAI tien trinh 'microk8s kubectl' - do duoc 0.85s tren may nay. Dashboard liet 40 bao cao la
# 40 lan hoi cung mot dia chi = ~34s cho mot trang, gan nhu toan bo thoi gian tai trang.
# KHONG cache vinh vien duoc: pod datanode doi IP moi khi khoi dong lai. Nen TTL ngan, cong
# them huy cache khi request toi datanode that bai (xem invalidate_datanode_cache).
_DATANODE_TTL_SEC = int(os.getenv("HDFS_DATANODE_CACHE_TTL_SEC", "300"))
# That bai cung phai nho, nhung ngan hon: khong nho thi o mot cum khong co pod datanode, MOI lan
# doc file lai chay 'microk8s kubectl' va goi K8s API roi moi chiu dung URL goc.
_DATANODE_MISS_TTL_SEC = 60
_datanode_cache = None   # (hostname, pod_ip, resolved_at_monotonic); hostname None = that bai


def invalidate_datanode_cache() -> None:
    """Quen dia chi datanode dang cache.

    Goi khi mot request toi datanode that bai: kha nang cao pod da khoi dong lai va doi IP,
    nen ban cache dang giu la rac. Lan phan giai sau se hoi lai tu dau.
    """
    global _datanode_cache
    _datanode_cache = None


def _resolve_datanode_cached():
    """(hostname, pod_ip), co cache. Tra None khi khong duong nao phan giai duoc.

    Dung time.monotonic() chu khong phai time.time(): dong ho he thong co the bi chinh lui
    (NTP), luc do TTL tinh ra am va cache song mai.
    """
    global _datanode_cache
    if not HDFS_DATANODE_REWRITE:
        return None
    if _datanode_cache is not None:
        hostname, pod_ip, resolved_at = _datanode_cache
        ttl = _DATANODE_TTL_SEC if hostname else _DATANODE_MISS_TTL_SEC
        if time.monotonic() - resolved_at < ttl:
            return (hostname, pod_ip) if hostname else None
    for lookup in (_resolve_datanode_pod_ip, _resolve_datanode_via_k8s_api):
        try:
            resolved = lookup()
        except Exception:   # thu chien luoc tiep theo
            continue
        if resolved:
            _datanode_cache = (resolved[0], resolved[1], time.monotonic())
            return resolved
    _datanode_cache = (None, None, time.monotonic())
    return None


def _redirect_target(location: str) -> str:
    """Rewrite a WebHDFS redirect so it is reachable from wherever this code is running.

    The datanode advertises its POD NAME (e.g. hdfs-datanode-854cddcb6c-7wjlr:9864). There is no
    Service for the datanode, so that name resolves NOWHERE -- not from the host and not from
    another pod. The pod IP therefore has to be substituted in both cases; only the way to look
    it up differs: kubectl on the host, the Kubernetes API inside a pod.

    Falls back to the untouched URL if neither lookup works, so a failed read degrades the same
    way it always did rather than raising.
    """
    resolved = _resolve_datanode_cached()
    if resolved:
        hostname, pod_ip = resolved
        return location.replace(f"http://{hostname}:9864", f"http://{pod_ip}:9864")
    if HDFS_DATANODE_REWRITE:
        print("WARNING: khong resolve duoc IP datanode (ca kubectl lan k8s API) - dung URL goc")
    return location


def write_json(hdfs_path: str, data: dict, overwrite: bool = True) -> bool:
    """Write a JSON document to HDFS via WebHDFS (two-step CREATE: namenode redirect ->
    datanode PUT). Creates parent directories implicitly (WebHDFS CREATE default). Returns
    True on success; raises on failure (caller should decide whether that's fatal -- this
    module treats "could not publish to HDFS" as worth surfacing loudly, unlike the read-side
    functions in this file which degrade gracefully, since a silently-not-published rule set
    means the Java job runs DQ checks against stale or missing rules without any signal.
    """
    payload = json.dumps(data, indent=2, ensure_ascii=False).encode("utf-8")
    if local_store_on():
        _local_write(hdfs_path, payload, overwrite)
        return True
    if trino_store_on():
        if not _TRINO_WRITE_ON:
            raise RuntimeError(f"DQ_STORE_TRINO dang CHI DOC - khong ghi {hdfs_path} "
                               "(bat DQ_STORE_TRINO_WRITE=1 o tien trinh can ghi)")
        # Nem loi nhu nhanh WebHDFS: ghi khong duoc phai keu to (xem docstring).
        _trino_write(hdfs_path, payload.decode("utf-8"))
        return True

    create_url = (
        f"{WEBHDFS_URL}/webhdfs/v1{hdfs_path}"
        f"?op=CREATE&overwrite={'true' if overwrite else 'false'}&user.name={HDFS_WEBHDFS_USER}"
    )
    req = urllib.request.Request(create_url, method="PUT")
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            location = resp.headers.get("Location") or resp.geturl()
    except urllib.error.HTTPError as e:
        # Expected: urllib correctly refuses to auto-follow a 307 carrying a PUT body (per
        # RFC 7231, a redirect that might resubmit a body needs explicit confirmation) --
        # the Location header is still on the exception, same shape as a normal response.
        if e.code != 307:
            raise
        location = e.headers.get("Location")
    if not location:
        raise RuntimeError(f"WebHDFS CREATE for {hdfs_path} returned no Location header to redirect to.")

    target = _redirect_target(location)

    def _put(url):
        put_req = urllib.request.Request(url, data=payload, method="PUT",
                                         headers={"Content-Type": "application/octet-stream"})
        with urllib.request.urlopen(put_req, timeout=30) as resp:
            return resp.status == 201

    try:
        return _put(target)
    except urllib.error.URLError:
        # Nhu ben doc: cache co the dang giu IP cua mot pod datanode da chet. Thu lai mot lan
        # voi dia chi moi; van hong thi nem ra, vi ghi that bai PHAI keu to (xem docstring).
        invalidate_datanode_cache()
        retarget = _redirect_target(location)
        if retarget == target:
            raise
        return _put(retarget)


def read_text(hdfs_path: str):
    """Read a UTF-8 file from HDFS via WebHDFS OPEN. Returns None if not found/unreachable.

    Like write_json, OPEN redirects (307, same as CREATE) to the datanode's in-cluster
    hostname, unresolvable from this host. Disable auto-follow so we can substitute the pod IP
    ourselves, same as write.
    """
    if local_store_on():
        return _local_read(hdfs_path)
    if trino_store_on():
        try:
            return _trino_read(hdfs_path)
        except Exception as e:
            print(f"WARNING: dq_store (Trino) read that bai cho {hdfs_path}: {e}", file=sys.stderr)
            return None
    url = f"{WEBHDFS_URL}/webhdfs/v1{hdfs_path}?op=OPEN&user.name={HDFS_WEBHDFS_USER}"
    opener = urllib.request.build_opener(_NoRedirect)
    try:
        try:
            opener.open(url, timeout=15)
            print(f"WARNING: WebHDFS OPEN for {hdfs_path} did not redirect as expected.")
            return None
        except urllib.error.HTTPError as e:
            if e.code not in (302, 307):
                raise
            location = e.headers.get("Location")

        target = _redirect_target(location)
        try:
            with urllib.request.urlopen(target, timeout=15) as response:
                return response.read().decode("utf-8")
        except urllib.error.URLError:
            # Gan nhu chac chan la IP trong cache da cu (pod datanode khoi dong lai). Quen di,
            # phan giai lai roi thu DUNG MOT LAN. Dia chi khong doi thi nem lai de except
            # ngoai xu ly y het truoc day.
            invalidate_datanode_cache()
            retarget = _redirect_target(location)
            if retarget == target:
                raise
            with urllib.request.urlopen(retarget, timeout=15) as response:
                return response.read().decode("utf-8")
    except (urllib.error.HTTPError, urllib.error.URLError) as e:
        print(f"WARNING: WebHDFS OPEN failed for {hdfs_path}: {e}")
        return None


def read_json(hdfs_path: str):
    """Read a JSON document from HDFS. None if missing, unreachable, or not valid JSON."""
    raw = read_text(hdfs_path)
    if raw is None:
        return None
    try:
        return json.loads(raw)
    except ValueError as e:
        print(f"WARNING: {hdfs_path} khong phai JSON hop le: {e}")
        return None


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        return None
