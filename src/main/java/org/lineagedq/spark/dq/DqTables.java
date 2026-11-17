package org.lineagedq.spark.dq;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

/**
 * Schema cua 11 bang Iceberg, khai bao mot cho. DDL sinh tu chinh cac StructType nay.
 *
 * Anh xa tu ban Postgres cu (dq-lineage-pipeline/jobs/dq_core/db.py): giu nguyen ten, tru
 * dq_reports + dq_errors gop thanh dq_report_history. Hai bang lineage la moi.
 *
 * Khoa la dq_key chu khong phai pipeline_name + pipeline_step: framework kiem DQ tren MOT bang.
 */
public final class DqTables {

    private DqTables() {
    }

    private static StructType s() {
        return new StructType();
    }

    private static StructType str(StructType t, String... names) {
        StructType out = t;
        for (String n : names) out = out.add(n, DataTypes.StringType);
        return out;
    }

    /** Bo rule da dung cho mot lan chay. `attached` la cho DUY NHAT thay rule bi guard bo di. */
    public static final DqTable RULES = DqTable.of("dq_rules",
            str(s(), "generation_run_id", "dq_key", "dataset_name", "rule_id", "column_name",
                    "description", "failed_sql_expr")
                    .add("importance_weight", DataTypes.IntegerType)
                    .add("attached", DataTypes.BooleanType)
                    .add("is_active", DataTypes.BooleanType)
                    .add("created_at", DataTypes.StringType)
                    // Rule DA mang stage/kind/observed_expr/expected trong active.json nhung
                    // truoc day mat sach khi vao bang - khong SQL duoc "checkpoint nay co rule
                    // nao". Cac cot moi de o CUOI vi ALTER TABLE ADD COLUMN luon them vao cuoi.
                    //   ALTER TABLE {catalog}.{db}.dq_rules ADD COLUMN stage STRING;
                    //   ALTER TABLE {catalog}.{db}.dq_rules ADD COLUMN kind STRING;
                    //   ALTER TABLE {catalog}.{db}.dq_rules ADD COLUMN observed_expr STRING;
                    //   ALTER TABLE {catalog}.{db}.dq_rules ADD COLUMN expected STRING;
                    //   ALTER TABLE {catalog}.{db}.dq_rules ADD COLUMN observed_unit STRING;
                    .add("stage", DataTypes.StringType)
                    .add("kind", DataTypes.StringType)
                    .add("observed_expr", DataTypes.StringType)
                    .add("expected", DataTypes.StringType)
                    // Don vi cua observed_expr: rows | percent | value. Khong co no thi doc
                    // observed_value ra mot con so tran, khong biet 9 la 9 dong hay 9 phan tram.
                    .add("observed_unit", DataTypes.StringType));

    /** Dung mot dong moi dq_key - ban cu dat PRIMARY KEY o day. */
    public static final DqTable LINEAGE_STATE = DqTable.of("dq_lineage_state",
            str(s(), "dq_key", "dataset_name", "lineage_hash", "generation_run_id", "updated_at"));

    /** 1 dong / rule / lan chay. Gop dq_reports + dq_errors cua ban cu. */
    /**
     * Cot `stage` la checkpoint da do ra dong nay; rong voi pipeline chua dung gate().
     * Bang da ton tai truoc khi them cot nay can chay mot lan:
     *   ALTER TABLE {catalog}.{db}.dq_report_history ADD COLUMN stage STRING;
     * Chua chay thi append bi tu choi va IcebergSink nuot loi - job khong sao, chi thieu dong.
     */
    public static final DqTable REPORT_HISTORY = DqTable.of("dq_report_history",
            str(s(), "run_id", "executed_at", "dq_key", "dataset_name", "status")
                    .add("quality_score", DataTypes.DoubleType)
                    .add("total_rows", DataTypes.LongType)
                    .add("generation_run_id", DataTypes.StringType)
                    .add("rule_id", DataTypes.StringType)
                    .add("column_name", DataTypes.StringType)
                    .add("importance_weight", DataTypes.LongType)
                    .add("violation_count", DataTypes.LongType)
                    .add("violation_pct", DataTypes.DoubleType)
                    // `stage` o CUOI, khong phai giua: bang co san duoc vá bang ALTER TABLE ADD
                    // COLUMN, ma ALTER luon them vao cuoi. De o giua thi schema Java va schema
                    // tren kho lech vi tri.
                    .add("stage", DataTypes.StringType)
                    // Khong co hai cot nay thi SQL khong phan biet duoc "do sach 75 rule" voi
                    // "khai 75 ma chi do duoc 3" - ca hai deu ra status PASS.
                    //   ALTER TABLE ... ADD COLUMN rules_declared BIGINT;
                    //   ALTER TABLE ... ADD COLUMN rules_attached BIGINT;
                    //   ALTER TABLE ... ADD COLUMN quality_score_trusted DOUBLE;
                    //   ALTER TABLE ... ADD COLUMN suspect_always_violating BOOLEAN;
                    //   ALTER TABLE ... ADD COLUMN kind STRING;
                    //   ALTER TABLE ... ADD COLUMN observed_value DOUBLE;
                    //   ALTER TABLE ... ADD COLUMN expected STRING;
                    //   ALTER TABLE ... ADD COLUMN observed_unit STRING;
                    //   ALTER TABLE ... ADD COLUMN pct_derived_from_observed BOOLEAN;
                    .add("rules_declared", DataTypes.LongType)
                    .add("rules_attached", DataTypes.LongType)
                    .add("quality_score_trusted", DataTypes.DoubleType)
                    .add("suspect_always_violating", DataTypes.BooleanType)
                    .add("kind", DataTypes.StringType)
                    .add("observed_value", DataTypes.DoubleType)
                    .add("expected", DataTypes.StringType)
                    .add("observed_unit", DataTypes.StringType)
                    // Phan biet "1.88% do duoc that" voi "100% vi quy uoc nhi phan". Khong co
                    // co nay thi doc bang khong biet con so nao la phep do, con so nao la dau
                    // hieu co/khong.
                    .add("pct_derived_from_observed", DataTypes.BooleanType));

    /**
     * Manifest checkpoint: 1 dong / cot / checkpoint / lan chay.
     *
     * Truoc day manifest chi nam o /dq/stages/{key}/latest.json, nen khong the SQL duoc
     * "pipeline co nhung checkpoint nao, cot gi, kieu gi" - ma do la truc chinh cua ca mo hinh
     * per-stage. Dashboard cung khong co gi de dung so do checkpoint neu thieu bang nay.
     */
    public static final DqTable STAGE_MANIFEST = DqTable.of("dq_stage_manifest",
            str(s(), "run_id", "captured_at", "dq_key", "dataset_name", "stage")
                    .add("stage_order", DataTypes.LongType)
                    .add("column_name", DataTypes.StringType)
                    .add("column_type", DataTypes.StringType)
                    .add("is_duplicate_name", DataTypes.BooleanType)
                    .add("row_count", DataTypes.LongType));

    /**
     * Phan quyet tham dinh theo TUNG checkpoint.
     *
     * verdict tong da co trong /dq/rules/{key}/verification/, nhung per_stage thi chi nam
     * trong JSON. Khong co bang nay thi khong tra duoc "checkpoint nao tung bi cham FAIL".
     */
    public static final DqTable RULE_VERIFICATION = DqTable.of("dq_rule_verification",
            str(s(), "generation_run_id", "dq_key", "dataset_name", "verified_at",
                    "overall_verdict", "stage", "stage_verdict", "notes"));

    public static final DqTable ANALYSIS = DqTable.of("dq_analysis",
            str(s(), "analysis_id", "dq_key", "report_run_id", "analysis_type", "content", "saved_at"));

    /** 1 dong / checkpoint. */
    public static final DqTable AGENT_RUNS = DqTable.of("dq_agent_runs",
            str(s(), "run_id", "dq_key", "checkpoint", "action", "reason", "recorded_at")
                    .add("num_tool_calls", DataTypes.LongType));

    /** 1 dong / tool call, theo dung thu tu agent goi. */
    public static final DqTable AGENT_TOOL_CALLS = DqTable.of("dq_agent_tool_calls",
            str(s(), "run_id", "dq_key", "checkpoint")
                    .add("sequence_no", DataTypes.LongType)
                    .add("tool_name", DataTypes.StringType)
                    .add("arguments_json", DataTypes.StringType)
                    .add("result_json", DataTypes.StringType)
                    .add("result_truncated", DataTypes.BooleanType));

    /** 1 dong / luot sinh rule hoac tham dinh rule. */
    public static final DqTable AGENT_RUN_ATTEMPTS = DqTable.of("dq_agent_run_attempts",
            str(s(), "run_id", "dq_key", "checkpoint")
                    .add("attempt_no", DataTypes.LongType)
                    .add("kind", DataTypes.StringType)
                    .add("ok", DataTypes.BooleanType)
                    .add("status", DataTypes.StringType)
                    .add("summary", DataTypes.StringType)
                    .add("regen_count", DataTypes.LongType)
                    .add("rules_generated_count", DataTypes.LongType)
                    .add("created_at", DataTypes.StringType));

    /** 1 dong / lan chay. raw_event_json giu nguyen event de lam phang lai duoc ve sau. */
    public static final DqTable LINEAGE_RUNS = DqTable.of("dq_lineage_runs",
            str(s(), "run_id", "ol_run_id", "job_namespace", "job_name", "event_type",
                    "event_time", "dq_key", "input_datasets", "output_datasets",
                    "raw_event_json", "captured_at"));

    /**
     * 1 dong / cap (cot dich, cot nguon).
     * scope = FIELD   : cot dich sinh TRUC TIEP tu cot nguon
     * scope = DATASET : anh huong GIAN TIEP (GROUP_BY/JOIN/FILTER), output_field de null
     */
    public static final DqTable COLUMN_LINEAGE = DqTable.of("dq_column_lineage",
            str(s(), "run_id", "dq_key", "scope", "output_namespace", "output_dataset",
                    "output_field", "input_namespace", "input_dataset", "input_field",
                    "transformation_type", "transformation_subtype", "transformation_description")
                    .add("masking", DataTypes.BooleanType)
                    .add("captured_at", DataTypes.StringType));
}
