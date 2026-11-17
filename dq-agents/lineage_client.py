"""
Doc lineage cot va schema tu chinh cac file framework tu ghi tren HDFS.

KHONG doc Marquez: Marquez la lop hien thi cho nguoi dung cuoi, khong phai nguon du lieu
cua core. Lineage lay tu /dq/lineage (NDJSON OpenLineage do listener ghi trong chinh lan
chay), schema lay tu manifest /dq/stages, lich su row-count lay tu /dq/reports/history.

Ported from dq-lineage-pipeline/jobs/dq_core/gen_rules.py (get_column_lineage,
get_dataset_schema, _parse_lineage_fields, _collapse_source_dataset), adapted for this
pipeline's actual Marquez registration:
  - namespace is the physical warehouse URI (hdfs://...), not "s3://lakehouse"
  - dataset name is the physical path (/warehouse/<db>.db/<table>), not a logical name
  - KHONG doc Marquez: moi nguon deu la file do chinh framework ghi ra HDFS.

get_downstream_datasets in the original connects directly to marquez-db Postgres with
hardcoded credentials — not ported here (out of scope for this pipeline: subscriber_call_daily
has no known downstream consumer registered in Marquez yet). Returns [] with a clear note
instead of silently pretending to have checked.
"""
import json
import os
import re
import urllib.error
import urllib.parse
import urllib.request

_DATA_FILE_EXTS = (".csv", ".parquet", ".json", ".orc", ".avro", ".txt")


def _collapse_source_dataset(name: str) -> str:
    """Collapse a source dataset path to its logical form (drop date/file partitions)."""
    kept = []
    for seg in name.split('/'):
        low = seg.lower()
        if low.endswith(_DATA_FILE_EXTS):
            continue
        if re.fullmatch(r'\d{6,}', seg):
            continue
        if '=' in seg and re.search(r'\d', seg):
            continue
        kept.append(seg)
    return '/'.join(kept) if kept else name


# _fetch_dataset() da bi XOA: no doc REST API cua Marquez, ma Marquez la lop hien thi cho
# nguoi dung cuoi chu khong phai nguon du lieu cua core. Moi thu core can deu do chinh
# framework ghi ra HDFS:
#     lineage       /dq/lineage/{key}/latest.json      (DqGuard.publishLineage)
#     schema        /dq/stages/{key}/latest.json       (DqGuard.publishStageManifest)
#     so dong       /dq/reports/{key}/history/*.json   (DqReportWriter.publish)


def _parse_lineage_fields(fields_dict: dict) -> dict:
    """Build {column: {derived_from: [...], transformation: str}} from a columnLineage-shaped
    'fields' dict. Prefers per-transformation description (extendedColumnLineage, nested
    transformations[]) and falls back to the flattened top-level transformationDescription
    (columnLineage 1-0-1) when nested transformations aren't present.
    """
    context = {}
    for col_name, col_data in fields_dict.items():
        input_fields = col_data.get('inputFields', [])
        sources = []
        transform_descriptions = []
        kinds = set()      # tap subtype: CONDITIONAL / WINDOW / AGGREGATION / IDENTITY...

        top_level_desc = col_data.get('transformationDescription')
        if top_level_desc and top_level_desc not in ("IDENTITY", "UNKNOWN", ""):
            transform_descriptions.append(top_level_desc)

        for inp in input_fields:
            ds_name = _collapse_source_dataset(inp.get('name', inp.get('dataset', 'unknown')))
            source_col = f"{ds_name}.{inp.get('field', 'unknown')}"
            sources.append(source_col)
            for t in inp.get('transformations', []):
                desc = t.get('description')
                subtype = t.get('subtype')
                if subtype:
                    kinds.add(subtype)
                if desc and desc not in ("IDENTITY", "UNKNOWN", ""):
                    transform_descriptions.append(desc)
                elif subtype and subtype != "IDENTITY":
                    transform_descriptions.append(subtype)

        sources = list(dict.fromkeys(sources))
        # SAP XEP truoc khi noi: mang transformations[] cua OpenLineage khong co thu tu on
        # dinh - hai lan chay cua cung mot job tra ve cung mot TAP bien doi nhung khac thu tu.
        # Noi theo thu tu mang cho ra hai chuoi khac nhau, va vi _compute_lineage_hash bam
        # tren chuoi nay nen lineage_changed_since_rules luon True: pipeline regen lai toan bo
        # rule moi lan chay ma khong bao gio hoi tu. Do that tren bang 79 cot, 4 cot bi anh
        # huong (list_lac_sac, end_lac, end_sac, end_province - deu qua explode/collect_list).
        unique_descs = sorted(dict.fromkeys(transform_descriptions))
        final_desc = " AND ".join(unique_descs) if unique_descs else "IDENTITY"
        entry = {"derived_from": sources, "transformation": final_desc}
        # CO Y KHONG dua `masking` cua facet ra: da kiem tren du lieu that, no KHONG nam tren
        # ba cot SHA-256 (cell_name/enb_name/sector) ma nam tren cac cot chi so phai sinh. Tuc
        # no khong co nghia "da an danh" nhu ten goi ra. Mot co ma ta khong giai thich chac
        # duoc thi khong duoc dua cho agent - dua mot tin hieu hieu sai chinh la cach 5 rule
        # NOT NULL sai da sinh ra.
        #
        # `kinds` thi CO SAN trong facet va truoc day bi vut di - chi giu lai
        # `description`. Do la mat mat that: dem tren cell_capacity_daily co 294 transformation
        # tren 38 cot, trong do CONDITIONAL 181 lan. `CONDITIONAL` chinh la tin hieu "cot nay
        # tinh co dieu kien nen NULL co the hop le" - dung thu thieu de khong sinh 5 rule
        # NOT NULL sai 22/09/2026. Mot chuoi mo ta dai 373 KB khong noi duoc dieu do, mot nhan
        # mot tu thi noi duoc.
        if kinds:
            entry["kinds"] = sorted(kinds)
        context[col_name] = entry
    return context


def _fetch_facet_from_hdfs(dataset_name: str) -> dict:
    """Doc facet columnLineage tu /dq/lineage/{key}/latest.json (su kien goc, job luu xuong)."""
    try:
        from dq_store import lineage_latest_path
        from hdfs_client import read_json
        key = dataset_name.rstrip('/').split('/')[-1]
        doc = read_json(lineage_latest_path(key)) or {}
        facet = doc.get('column_lineage') or {}
        if facet.get('fields'):
            print(f"Lineage tu HDFS: {lineage_latest_path(key)} "
                  f"(run {doc.get('run_id')}, {len(facet['fields'])} cot)")
            return facet
    except Exception as e:  # con duong Marquez ben duoi
        print(f"Khong doc duoc lineage tu HDFS: {e}")
    return {}


def get_column_lineage(dataset_name: str) -> dict:
    """Lay column lineage tu /dq/lineage/{key}/latest.json - su kien GOC do listener ghi ra.

    MOT nguon duy nhat, KHONG co duong lui sang Marquez. Marquez la lop hien thi cho nguoi
    dung cuoi; core khong duoc lay input tu no. Duong lui cu con nguy hiem hon la khong co:
    HDFS trong thi no am tham doi nguon, va bo rule sinh ra dua tren mot ban lineage khac -
    khong ai biet ban nao da duoc dung.

    Khong doc duoc thi tra {} va NOI RO. Ben goi phai coi day la 'khong co lineage', khong
    phai 'khong co bien doi nao'.

    Tu 1.53.0 columnLineage da mang bieu thuc Catalyst that nen agent khong phu thuoc buoc
    reshape; extendedColumnLineage giu lai chi de doc duoc su kien cu.
    """
    col_facet = _fetch_facet_from_hdfs(dataset_name)
    ext_facet = {}
    if not col_facet:
        print(f"Khong co lineage tai /dq/lineage/ cho {dataset_name}"
              f" - tra ve rong. KHONG doi sang Marquez: do la lop hien thi, khong phai nguon.")
        return {}

    column_lineage_context = _parse_lineage_fields(col_facet.get('fields', {}))
    if not column_lineage_context:
        column_lineage_context = _parse_lineage_fields(ext_facet.get('fields', {}))

    # INDIRECT dataset-level dependencies (JOIN/FILTER/GROUP_BY/WINDOW) — affect every row
    # even though the source column isn't in any single output column's DIRECT lineage.
    indirect_deps = []
    seen = set()
    # dataset[] sits in the raw facet already; the reshape step never touches it.
    for dep in (col_facet.get('dataset') or ext_facet.get('dataset') or []):
        dep_ds = _collapse_source_dataset(dep.get('name', dep.get('dataset', 'unknown')))
        source = f"{dep_ds}.{dep.get('field', 'unknown')}"
        for t in dep.get('transformations', []):
            if t.get('type') == 'INDIRECT' and t.get('subtype'):
                key = (source, t['subtype'])
                if key not in seen:
                    seen.add(key)
                    indirect_deps.append({"source": source, "subtype": t['subtype']})
    if indirect_deps:
        column_lineage_context["__indirect_impact__"] = indirect_deps

    # Cac cot tung xuat hien trong mot GROUP_BY o dau do trong pipeline. Day la KHOA UNG VIEN
    # chu KHONG phai hat xac dinh: tren cell_capacity_daily no tra ve
    # [cell_name, date_hour, enb_name, sector] - tron ca phan hoach khu trung lap theo gio lan
    # khoa gom theo ngay, hai phep GROUP_BY khac nhau. Agent phai doc no nhu goi y, va tin
    # hieu that o day la "hat DOI giua cac chang".
    # Thong tin nay nam san trong facet va truoc day bi bo: ta chi lay (source, subtype) roi
    # khu trung lap. Thieu no, agent chi co mot phat bieu ve hat (cau trong business metadata,
    # noi ve bang DAU RA) va ap no o moi checkpoint - sinh ra TF_CONTRACT_UNIQUE vi pham
    # 95.75% vi tf_contract la du lieu THEO GIO. 22/09/2026.
    group_by_cols = sorted({d["source"].split(".")[-1] for d in indirect_deps
                            if d["subtype"] == "GROUP_BY"})
    if group_by_cols:
        column_lineage_context["__group_by_columns__"] = group_by_cols

    return column_lineage_context


def get_dataset_schema(dataset_name: str) -> dict:
    """{ten_cot: kieu} de schema-grounding, gop tu MANIFEST CHECKPOINT.

    Truoc day ham nay doc Marquez. Sai ve nguyen tac: Marquez la lop HIEN THI cho nguoi dung
    cuoi, khong phai nguon du lieu cua core - Marquez chet thi self-check cua Gen Rules im
    lang khong co schema ma kiem, va khong ai biet.

    Manifest con CHINH XAC HON: job Java chup no bang df.schema() ngay tai tung checkpoint,
    nen no co ca cot trung gian (imsi_new, location_report_payload...) von khong bao gio ton
    tai trong bang dich ma Marquez biet. Gop moi stage lai, stage SOM thang - mot cot xuat
    hien tu src_calls thi kieu o do la kieu goc.

    {} nghia la 'khong kiem duoc' (chua co manifest), ben goi PHAI coi day la khong chan,
    khong phai la 'schema rong' - neu khong se tu choi sach moi rule.
    """
    try:
        from dq_store import get_stages
        doc = get_stages(_dq_key(dataset_name)) or {}
    except Exception as e:
        print(f"Khong doc duoc manifest de lay schema: {e}")
        return {}
    schema = {}
    for stage in doc.get("stages", []):
        for column in stage.get("schema") or []:
            name = column.get("name")
            if name and name not in schema:          # stage som thang
                schema[name] = column.get("type", "")
    return schema


def _dq_key(dataset_name: str) -> str:
    """Suy dq_key tu ten dataset - cung quy uoc voi gen_rules_agent._dq_key."""
    import os
    key = os.getenv("DQ_KEY")
    if key:
        return key
    return dataset_name.rstrip("/").split("/")[-1].replace(".db", "")


def get_downstream_datasets(dataset_name: str) -> list:
    """Not wired to a downstream-lineage query in this pipeline yet — the original queries
    marquez-db Postgres directly with hardcoded credentials, which we're deliberately not
    reproducing here. subscriber_call_daily currently has no known downstream consumer
    registered in Marquez, so an honest empty list is correct today; wire this up (via the
    Marquez column-lineage graph API, not a raw DB connection) if/when a downstream job reads
    this table.
    """
    return []


def get_row_count_history(dataset_name: str, namespace: str = None, limit: int = 10) -> list:
    """Lich su so dong qua cac lan chay, doc tu REPORT do chinh framework ghi.

    Truoc day ham nay goi REST API cua Marquez. Sai ve nguyen tac: Marquez la lop hien thi cho
    nguoi dung cuoi, khong phai nguon du lieu cua core. No cung tung hong that - endpoint
    /api/v1/events/lineage tra 500 "Ran out of memory" khi limit=50.

    Nguon thay the co san va chinh xac hon: /dq/reports/{key}/history/{run_id}.json do
    DqReportWriter.publish ghi moi lan chay, trong do moi stage mang total_rows cua RIENG no.
    Marquez chi biet so dong cua BANG DICH; report biet so dong o TUNG CHECKPOINT, nen phat
    hien duoc bat thuong kieu "nguon van du 500 dong nhung sau tf_decode chi con 12".

    @param namespace giu lai cho tuong thich chu ky, khong con dung.
    """
    try:
        from dq_store import reports_latest_path
        from hdfs_client import list_dir, read_json
    except Exception as e:
        print(f"Khong nap duoc HDFS client de doc lich su: {e}")
        return []

    key = _dq_key(dataset_name)
    base = reports_latest_path(key).rsplit("/", 1)[0] + "/history"
    try:
        names = sorted(list_dir(base) or [], reverse=True)[:limit]
    except Exception as e:
        print(f"Khong liet ke duoc {base}: {e}")
        return []

    history = []
    for name in names:
        doc = read_json(f"{base}/{name}")
        if not doc:
            continue
        stages = doc.get("stages") or []
        # Stage CUOI la dataframe ngay truoc lenh ghi - tuong duong so dong cua bang dich ma
        # Marquez tung bao, nhung con kem theo so dong cua moi chang truoc do.
        final_rows = stages[-1].get("total_rows") if stages else doc.get("total_rows")
        history.append({
            "run_id": doc.get("run_id"),
            "executed_at": doc.get("executed_at"),
            "row_count": final_rows,
            "per_stage": {s.get("stage"): s.get("total_rows") for s in stages},
        })
    return history
