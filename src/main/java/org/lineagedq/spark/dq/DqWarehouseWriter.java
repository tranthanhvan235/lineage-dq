package org.lineagedq.spark.dq;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Lam phang tai lieu JSON trong /dq/ thanh dong roi day qua IcebergSink.
 *
 * JSON van la nguon su that; bang Iceberg chi de SELECT/GROUP BY. Schema o DqTables, viec tao
 * bang va nuot loi o IcebergSink.
 */
public final class DqWarehouseWriter {

    private static final Logger LOG = LoggerFactory.getLogger(DqWarehouseWriter.class);

    private DqWarehouseWriter() {
    }

    /**
     * Lam phang report cua MOT checkpoint thanh 1 dong / rule.
     *
     * Stage KHONG CO rule nao van sinh dung mot dong tom tat (rule_id = null). Truoc day vong
     * lap chay tren mang `rules`, nen mang rong = khong dong nao: stage hong nang nhat - khai
     * 75 rule ma rot sach, hoac chua rule nao - lai la stage DUY NHAT vo hinh voi cau
     * `WHERE status IN ('FAIL','WARNING')`. Dung kieu sai am tham ma ca framework nay tranh.
     */
    public static void appendReport(IcebergSink sink, String dqKey, JsonNode report) {
        if (report == null) return;
        List<Row> rows = new ArrayList<>();
        for (JsonNode r : report.path("rules")) {
            rows.add(reportRow(report, dqKey, r));
        }
        if (rows.isEmpty()) {
            rows.add(reportRow(report, dqKey, null));
        }
        sink.append(DqTables.REPORT_HISTORY, rows);
    }

    /** Mot dong report. rule = null nghia la dong tom tat cua stage khong do duoc rule nao. */
    private static Row reportRow(JsonNode report, String dqKey, JsonNode rule) {
        boolean hasRule = rule != null;
        return RowFactory.create(
                text(report, "run_id"), text(report, "executed_at"), dqKey,
                text(report, "dataset_name"), text(report, "status"),
                report.hasNonNull("quality_score") ? report.get("quality_score").asDouble() : null,
                report.path("total_rows").asLong(),
                text(report, "checked_against_generation_run_id"),
                hasRule ? text(rule, "rule_id") : null,
                hasRule ? text(rule, "column") : null,
                hasRule ? rule.path("importance_weight").asLong() : null,
                hasRule ? rule.path("violation_count").asLong() : null,
                hasRule ? rule.path("violation_pct").asDouble() : null,
                text(report, "stage"),
                report.path("rules_declared").asLong(),
                report.path("rules_attached").asLong(),
                report.hasNonNull("quality_score_trusted")
                        ? report.get("quality_score_trusted").asDouble() : null,
                hasRule && rule.path("suspect_always_violating").asBoolean(false),
                hasRule ? text(rule, "kind") : null,
                hasRule && rule.hasNonNull("observed_value")
                        ? rule.get("observed_value").asDouble() : null,
                hasRule ? text(rule, "expected") : null,
                hasRule ? text(rule, "observed_unit") : null,
                hasRule && rule.path("pct_derived_from_observed").asBoolean(false));
    }

    /**
     * Manifest checkpoint: 1 dong / cot / checkpoint. De SQL duoc "pipeline co checkpoint nao,
     * cot gi, kieu gi" - truc chinh cua mo hinh per-stage, truoc day chi nam trong JSON.
     */
    public static void appendStageManifest(IcebergSink sink, String dqKey, JsonNode manifest) {
        if (manifest == null) return;
        List<Row> rows = new ArrayList<>();
        String runId = text(manifest, "run_id");
        String capturedAt = text(manifest, "captured_at");
        String datasetName = text(manifest, "dataset_name");
        for (JsonNode stage : manifest.path("stages")) {
            java.util.Set<String> duplicates = new java.util.HashSet<>();
            for (JsonNode d : stage.path("duplicate_names")) duplicates.add(d.asText());
            Long rowCount = stage.hasNonNull("row_count") ? stage.get("row_count").asLong() : null;
            for (JsonNode column : stage.path("schema")) {
                String name = text(column, "name");
                rows.add(RowFactory.create(
                        runId, capturedAt, dqKey, datasetName, text(stage, "stage"),
                        stage.path("order").asLong(), name, text(column, "type"),
                        duplicates.contains(name), rowCount));
            }
        }
        sink.append(DqTables.STAGE_MANIFEST, rows);
    }

    /** Phan quyet tham dinh theo tung checkpoint: 1 dong / stage. */
    public static void appendRuleVerification(IcebergSink sink, String dqKey, JsonNode verdict) {
        if (verdict == null) return;
        List<Row> rows = new ArrayList<>();
        String genId = text(verdict, "generation_run_id");
        String verifiedAt = text(verdict, "verified_at");
        String datasetName = text(verdict, "dataset_name");
        String overall = text(verdict, "verdict");
        String notes = text(verdict, "notes");
        JsonNode perStage = verdict.path("per_stage");
        java.util.Iterator<String> names = perStage.fieldNames();
        while (names.hasNext()) {
            String stage = names.next();
            rows.add(RowFactory.create(genId, dqKey, datasetName, verifiedAt, overall,
                    stage, perStage.path(stage).asText(), notes));
        }
        if (rows.isEmpty()) {
            // Verdict khong khai per_stage van phai co dau vet - neu khong thi mot lan tham
            // dinh "dat tong the ma khong chi tiet" bien mat hoan toan khoi kho.
            rows.add(RowFactory.create(genId, dqKey, datasetName, verifiedAt, overall,
                    null, null, notes));
        }
        sink.append(DqTables.RULE_VERIFICATION, rows);
    }

    /**
     * Ghi bo rule active ra dq_rules + dq_lineage_state. Idempotent theo generation_run_id.
     *
     * Goi SAU attach chu khong phai sau loadActive: chi luc do moi biet rule nao bi bo, va cot
     * `attached` la cho DUY NHAT thay dieu do (DqReportWriter chi duyet attachedRules).
     */
    public static void appendRules(IcebergSink sink, String dqKey, DqRuleSet ruleSet,
                                   List<DqRule> attachedRules) {
        if (ruleSet == null || ruleSet.isEmpty()) return;

        String where = "dq_key = " + IcebergSink.lit(dqKey)
                + " AND generation_run_id = " + IcebergSink.lit(ruleSet.generationRunId());
        if (sink.count(DqTables.RULES, where) > 0) {
            LOG.info("=== DqWarehouseWriter: dq_rules da co generation_run_id={} - bo qua",
                    ruleSet.generationRunId());
            return;
        }

        Set<String> attachedIds = new HashSet<>();
        if (attachedRules != null) {
            for (DqRule r : attachedRules) attachedIds.add(r.ruleId());
        }

        List<Row> rows = new ArrayList<>();
        for (DqRule r : ruleSet.rules()) {
            rows.add(RowFactory.create(ruleSet.generationRunId(), dqKey, ruleSet.datasetName(),
                    r.ruleId(), r.column(), r.description(), r.failedSqlExpr(),
                    r.importanceWeight(), attachedIds.contains(r.ruleId()), true,
                    ruleSet.generatedAt(),
                    // Rule DA mang bon truong nay trong active.json; truoc day chung mat sach
                    // khi vao bang, nen khong SQL duoc "checkpoint nay co rule nao".
                    r.stage(), r.kind(), r.observedExpr(), r.expected(), r.observedUnit()));
        }

        // Ha co is_active cua cac lan sinh truoc, de khong luc nao co hai bo cung active.
        sink.exec(DqTables.RULES, "UPDATE {t} SET is_active = false"
                + " WHERE dq_key = " + IcebergSink.lit(dqKey) + " AND is_active = true");
        sink.append(DqTables.RULES, rows);

        sink.exec(DqTables.LINEAGE_STATE, "DELETE FROM {t} WHERE dq_key = " + IcebergSink.lit(dqKey));
        sink.append(DqTables.LINEAGE_STATE, List.of(RowFactory.create(
                dqKey, ruleSet.datasetName(), ruleSet.lineageHash(),
                ruleSet.generationRunId(), ruleSet.generatedAt())));

        LOG.info("=== DqWarehouseWriter: {} rule ({} duoc do, {} bi bo)",
                rows.size(), attachedIds.size(), rows.size() - attachedIds.size());
    }

    /** /dq/checkpoints/ -> 3 bang. Goi SAU checkpoint cuoi de bat duoc moi checkpoint. */
    public static void appendAgentRuns(IcebergSink sink, Configuration hadoopConf,
                                       String hdfsBaseUri, String dqKey, String runId) {
        JsonNode doc = DqJson.read(hadoopConf, hdfsBaseUri,
                hdfsBaseUri + "/dq/checkpoints/" + dqKey + "/" + runId + ".json");
        if (doc == null) {
            LOG.warn("=== DqWarehouseWriter: chua co checkpoint doc cho run {} - bo qua", runId);
            return;
        }

        List<Row> runRows = new ArrayList<>();
        List<Row> callRows = new ArrayList<>();
        List<Row> attemptRows = new ArrayList<>();
        for (JsonNode c : doc.path("checkpoints")) {
            String cp = text(c, "checkpoint");
            JsonNode calls = c.path("tool_calls");
            runRows.add(RowFactory.create(runId, dqKey, cp, text(c, "action"),
                    text(c, "reason"), text(c, "recorded_at"), (long) calls.size()));

            for (JsonNode t : calls) {
                callRows.add(RowFactory.create(runId, dqKey, cp,
                        t.path("sequence_no").asLong(), text(t, "tool_name"),
                        text(t, "arguments_json"), text(t, "result_json"),
                        t.path("result_truncated").asBoolean()));
            }
            for (JsonNode a : c.path("attempts")) {
                attemptRows.add(RowFactory.create(runId, dqKey, cp,
                        a.path("attempt_no").asLong(), text(a, "kind"),
                        a.hasNonNull("ok") ? a.path("ok").asBoolean() : null,
                        text(a, "status"), text(a, "summary"),
                        a.hasNonNull("regen_count") ? a.path("regen_count").asLong() : null,
                        a.hasNonNull("rules_generated_count")
                                ? a.path("rules_generated_count").asLong() : null,
                        text(a, "at")));
            }
        }
        sink.append(DqTables.AGENT_RUNS, runRows);
        sink.append(DqTables.AGENT_TOOL_CALLS, callRows);
        sink.append(DqTables.AGENT_RUN_ATTEMPTS, attemptRows);
    }

    /**
     * Ghi ban phan tich. CHI khi report_run_id khop lan chay nay - latest.json luon ton tai sau
     * lan dau, khong kiem thi moi lan chay deu chep lai ban cu voi sai run_id.
     */
    public static void appendAnalysis(IcebergSink sink, Configuration hadoopConf,
                                      String hdfsBaseUri, String dqKey, String runId) {
        JsonNode doc = DqJson.read(hadoopConf, hdfsBaseUri,
                hdfsBaseUri + "/dq/analysis/" + dqKey + "/latest.json");
        if (doc == null) return;

        String reportRunId = text(doc, "report_run_id");
        if (reportRunId == null || !reportRunId.equals(runId)) {
            LOG.info("=== DqWarehouseWriter: analysis moi nhat thuoc run {} chu khong phai {}"
                    + " - bo qua (Analysis khong chay lan nay).", reportRunId, runId);
            return;
        }
        String analysisId = text(doc, "analysis_id");
        if (sink.count(DqTables.ANALYSIS, "analysis_id = " + IcebergSink.lit(analysisId)) > 0) {
            return;
        }
        sink.append(DqTables.ANALYSIS, List.of(RowFactory.create(
                analysisId, dqKey, reportRunId, text(doc, "analysis_type"),
                text(doc, "content"), text(doc, "saved_at"))));
    }

    private static String text(JsonNode n, String field) {
        return n.hasNonNull(field) ? n.get(field).asText() : null;
    }
}
