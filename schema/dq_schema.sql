-- SINH TU src/main/java/org/lineagedq/spark/dq/DqTables.java - DUNG SUA TAY.
-- Sinh lai: python3 scripts/gen_schema_docs.py --write
--
-- Thay {db} bang schema cua pipeline: warehouse_c | warehouse_b | warehouse_a.
-- Job tu tao bang khi ghi lan dau; file nay de DOC va de nap vao cong cu ve so do.

-- 1 dong / rule / luot sinh | khoa nghiep vu: generation_run_id, stage, rule_id
CREATE TABLE IF NOT EXISTS iceberg.{db}.dq_rules (
    generation_run_id varchar,
    dq_key            varchar,
    dataset_name      varchar,
    rule_id           varchar,
    column_name       varchar,
    description       varchar,
    failed_sql_expr   varchar,
    importance_weight integer,
    attached          boolean,
    is_active         boolean,
    created_at        varchar,
    stage             varchar,
    kind              varchar,
    observed_expr     varchar,
    expected          varchar,
    observed_unit     varchar
) WITH (format = 'PARQUET');

-- 1 dong / dq_key - rule hien tai ung voi lineage nao | khoa nghiep vu: dq_key
CREATE TABLE IF NOT EXISTS iceberg.{db}.dq_lineage_state (
    dq_key            varchar,
    dataset_name      varchar,
    lineage_hash      varchar,
    generation_run_id varchar,
    updated_at        varchar
) WITH (format = 'PARQUET');

-- 1 dong / rule / stage / lan chay | khoa nghiep vu: run_id, stage, rule_id
CREATE TABLE IF NOT EXISTS iceberg.{db}.dq_report_history (
    run_id                    varchar,
    executed_at               varchar,
    dq_key                    varchar,
    dataset_name              varchar,
    status                    varchar,
    quality_score             double,
    total_rows                bigint,
    generation_run_id         varchar,
    rule_id                   varchar,
    column_name               varchar,
    importance_weight         bigint,
    violation_count           bigint,
    violation_pct             double,
    stage                     varchar,
    rules_declared            bigint,
    rules_attached            bigint,
    quality_score_trusted     double,
    suspect_always_violating  boolean,
    kind                      varchar,
    observed_value            double,
    expected                  varchar,
    observed_unit             varchar,
    pct_derived_from_observed boolean
) WITH (format = 'PARQUET');

-- 1 dong / cot / stage / lan chay | khoa nghiep vu: run_id, stage, column_name
CREATE TABLE IF NOT EXISTS iceberg.{db}.dq_stage_manifest (
    run_id            varchar,
    captured_at       varchar,
    dq_key            varchar,
    dataset_name      varchar,
    stage             varchar,
    stage_order       bigint,
    column_name       varchar,
    column_type       varchar,
    is_duplicate_name boolean,
    row_count         bigint
) WITH (format = 'PARQUET');

-- 1 dong / stage / luot sinh | khoa nghiep vu: generation_run_id, stage
CREATE TABLE IF NOT EXISTS iceberg.{db}.dq_rule_verification (
    generation_run_id varchar,
    dq_key            varchar,
    dataset_name      varchar,
    verified_at       varchar,
    overall_verdict   varchar,
    stage             varchar,
    stage_verdict     varchar,
    notes             varchar
) WITH (format = 'PARQUET');

-- 1 dong / ban phan tich | khoa nghiep vu: analysis_id
CREATE TABLE IF NOT EXISTS iceberg.{db}.dq_analysis (
    analysis_id   varchar,
    dq_key        varchar,
    report_run_id varchar,
    analysis_type varchar,
    content       varchar,
    saved_at      varchar
) WITH (format = 'PARQUET');

-- 1 dong / checkpoint / lan chay | khoa nghiep vu: run_id, checkpoint
CREATE TABLE IF NOT EXISTS iceberg.{db}.dq_agent_runs (
    run_id         varchar,
    dq_key         varchar,
    checkpoint     varchar,
    action         varchar,
    reason         varchar,
    recorded_at    varchar,
    num_tool_calls bigint
) WITH (format = 'PARQUET');

-- 1 dong / lan goi tool | khoa nghiep vu: run_id, checkpoint, sequence_no
CREATE TABLE IF NOT EXISTS iceberg.{db}.dq_agent_tool_calls (
    run_id           varchar,
    dq_key           varchar,
    checkpoint       varchar,
    sequence_no      bigint,
    tool_name        varchar,
    arguments_json   varchar,
    result_json      varchar,
    result_truncated boolean
) WITH (format = 'PARQUET');

-- 1 dong / luot sinh lai rule | khoa nghiep vu: run_id, checkpoint, attempt_no
CREATE TABLE IF NOT EXISTS iceberg.{db}.dq_agent_run_attempts (
    run_id                varchar,
    dq_key                varchar,
    checkpoint            varchar,
    attempt_no            bigint,
    kind                  varchar,
    ok                    boolean,
    status                varchar,
    summary               varchar,
    regen_count           bigint,
    rules_generated_count bigint,
    created_at            varchar
) WITH (format = 'PARQUET');

-- 1 dong / su kien OpenLineage | khoa nghiep vu: run_id, ol_run_id, event_type
CREATE TABLE IF NOT EXISTS iceberg.{db}.dq_lineage_runs (
    run_id          varchar,
    ol_run_id       varchar,
    job_namespace   varchar,
    job_name        varchar,
    event_type      varchar,
    event_time      varchar,
    dq_key          varchar,
    input_datasets  varchar,
    output_datasets varchar,
    raw_event_json  varchar,
    captured_at     varchar
) WITH (format = 'PARQUET');

-- 1 dong / canh (cot dich, cot nguon) | khoa nghiep vu: run_id, output_field, input_field
CREATE TABLE IF NOT EXISTS iceberg.{db}.dq_column_lineage (
    run_id                     varchar,
    dq_key                     varchar,
    scope                      varchar,
    output_namespace           varchar,
    output_dataset             varchar,
    output_field               varchar,
    input_namespace            varchar,
    input_dataset              varchar,
    input_field                varchar,
    transformation_type        varchar,
    transformation_subtype     varchar,
    transformation_description varchar,
    masking                    boolean,
    captured_at                varchar
) WITH (format = 'PARQUET');
