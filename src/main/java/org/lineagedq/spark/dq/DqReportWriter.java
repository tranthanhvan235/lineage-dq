package org.lineagedq.spark.dq;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.List;
import java.util.Map;

/**
 * Tinh quality score tu ket qua aggregate cua DqMetricQuery va publish report len /dq/reports/{key}/ -
 * khu vuc rieng, tach biet /dq/rules/ va cang tach biet hon voi /warehouse (du lieu pipeline).
 *
 * Cong thuc giong quy uoc cu (dq-lineage-pipeline/jobs/dq_core/execute_col_rules.py):
 *   error_pct_i    = violation_count_i / total_rows * 100
 *   weighted_error = SUM(error_pct_i * weight_i) / SUM(weight_i)
 *   quality_score  = 1 - weighted_error / 100
 *   status         = FAIL neu quality_score < failThreshold (mac dinh 0.8)
 *                    WARNING neu < warningThreshold (mac dinh 0.95)
 *                    else PASS
 */
public final class DqReportWriter {

    private static final Logger LOG = LoggerFactory.getLogger(DqReportWriter.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private DqReportWriter() {
    }

    /**
     * Report cua MOT checkpoint. Diem chat luong tinh RIENG tung stage va KHONG duoc gop lai:
     * moi stage co mau so rieng (do tren pipeline nay: 500 dong nguon -> 1250 sau explode ->
     * 500 sau groupBy), nen cong/trung binh cac quality_score lai la so vo nghia.
     */
    public static ObjectNode buildStageReport(
            String stage,
            int order,
            String datasetName,
            String appId,
            DqRuleSet ruleSet,
            List<DqRule> attachedRules,
            Map<String, Object> observedMetrics,
            int rulesDeclared,
            Map<String, String> droppedRules) {

        ObjectNode r = buildReport(datasetName, appId, ruleSet, attachedRules, observedMetrics);

        // rules_declared canh rules_attached: rule bi bo luc probe truoc day BIEN MAT khoi ca
        // mau so, nen stage van bao 1.0 trong khi thuc te co rule khong chay duoc. Khi lech
        // nhau thi tran cua status la WARNING, du diem co dep den may.
        r.put("rules_declared", rulesDeclared);
        r.put("rules_attached", attachedRules.size());
        if (droppedRules != null && !droppedRules.isEmpty()) {
            ObjectNode d = r.putObject("rules_dropped");
            droppedRules.forEach(d::put);
            if ("PASS".equals(r.path("status").asText())) {
                r.put("status", "WARNING");
                r.put("status_capped_reason",
                        "co " + droppedRules.size() + " rule khai bao nhung KHONG do duoc");
            }
        }
        // Dat stage/order len dau cho de doc; Jackson giu thu tu chen.
        ObjectNode out = MAPPER.createObjectNode();
        out.put("stage", stage);
        out.put("order", order);
        out.setAll(r);
        out.remove("dataset_name");
        out.remove("run_id");
        return out;
    }

    /**
     * Gop cac report stage thanh mot tai lieu.
     *
     * quality_score cua ca lan chay = TRUNG BINH CONG diem TIN CAY cua cac checkpoint.
     *
     * Tin cay chu khong tho: trang thai tung chang cham theo quality_score_trusted, nen
     * diem cua ca luot phai dung cung co so - neu khong thi con so dau trang va trang thai
     * dau trang noi hai dieu khac nhau. quality_score_raw giu lai ban tho de doi chieu.
     *
     * Trung binh CONG chu khong phai trung binh co trong so theo so dong: moi checkpoint la
     * mot cau hoi rieng ve mot hinh dang du lieu rieng, va mot checkpoint 40 dong hong nang
     * khong duoc bien mat chi vi checkpoint canh no co 500 dong. Doi lai, no khong doc duoc
     * nhu "ty le du lieu sach" - de doc dung thi phai nhin quality_score cua tung chang.
     *
     * Checkpoint khong do duoc gi (quality_score = null) bi bo khoi ca tu va mau, chu khong
     * tinh la 0 hay 1: no khong mang thong tin nao ve chat luong. So chang thuc su gop nam
     * o scored_stage_count de nguoi doc biet trung binh nay tinh tren bao nhieu chang.
     *
     * worst_stage / worst_stage_score van giu: trung binh giau mat chang te nhat, va do
     * thuong la chang dang phai xu ly.
     */
    public static ObjectNode buildPipelineReport(
            String datasetName,
            String appId,
            DqRuleSet ruleSet,
            List<ObjectNode> stageReports) {

        ObjectNode doc = MAPPER.createObjectNode();
        doc.put("dataset_name", datasetName);
        doc.put("run_id", appId);
        doc.put("executed_at", Instant.now().toString());
        doc.put("checked_against_generation_run_id", ruleSet.generationRunId());
        doc.put("report_shape", "per_stage");

        ArrayNode arr = doc.putArray("stages");
        String worst = "PASS";
        double scoreSum = 0.0;
        double rawSum = 0.0;
        int scored = 0;
        Double worstScore = null;
        String worstStage = null;
        for (ObjectNode sr : stageReports) {
            arr.add(sr);
            worst = worseOf(worst, sr.path("status").asText("PASS"));
            if (!sr.hasNonNull("quality_score")) continue;
            // Lay diem TIN CAY (da bo rule dang ngo) chu khong phai diem tho: trang thai
            // cua tung chang cham theo chinh no, nen diem cua ca luot phai dung cung mot
            // co so. Da lech that - mot luot moi chang deu trusted=1.0 va chi bi ha xuong
            // WARNING vi co rule dang ngo, nhung diem luot chay hien 0.761 vi cong diem
            // tho. Con so dau trang va trang thai dau trang noi hai dieu khac nhau.
            double raw = sr.get("quality_score").asDouble();
            double score = sr.hasNonNull("quality_score_trusted")
                    ? sr.get("quality_score_trusted").asDouble() : raw;
            scoreSum += score;
            rawSum += raw;
            scored++;
            if (worstScore == null || score < worstScore) {
                worstScore = score;
                worstStage = sr.path("stage").asText(null);
            }
        }
        doc.put("stage_count", stageReports.size());
        doc.put("worst_status", worst);
        doc.put("scored_stage_count", scored);
        if (scored > 0) {
            doc.put("quality_score", Math.round(scoreSum / scored * 10000.0) / 10000.0);
            // Diem THO, chua bo rule dang ngo. Giu lai de con doi chieu duoc: chenh lech
            // giua hai so nay DUNG BANG phan ma rule dang ngo dang doi lay.
            doc.put("quality_score_raw", Math.round(rawSum / scored * 10000.0) / 10000.0);
            doc.put("worst_stage_score", worstScore);
            doc.put("worst_stage", worstStage);
        } else {
            doc.putNull("quality_score");
            doc.putNull("quality_score_raw");
            doc.putNull("worst_stage_score");
            doc.putNull("worst_stage");
        }
        // Nguong da dung de cham trang thai, ghi NGAY TRONG report. Khong co no thi mot
        // report cu doc lai sau khi nguong doi la mot mau thuan khong giai thich duoc: da
        // xay ra that - the pipeline hien FAIL canh diem 0.9688 trong khi nguong hien tai
        // la 0.9, va khong cach nao biet do la nguong cu hay la loi tinh diem.
        ObjectNode th = doc.putObject("thresholds");
        th.put("fail", DqConfig.QUALITY_FAIL_THRESHOLD);
        th.put("warning", DqConfig.QUALITY_WARNING_THRESHOLD);
        th.put("suspect_rule_pct", DqConfig.SUSPECT_RULE_PCT);
        return doc;
    }

    /**
     * Thu tu "te hon" giua hai status. MOT cho duy nhat dinh nghia thang nay.
     *
     * INSUFFICIENT_DATA va NO_RULES_APPLIED phai te hon PASS: mot checkpoint khong do duoc gi
     * thi khong biet du lieu tot hay xau, va bao PASS la noi doi. Truoc day vong tinh
     * worst_status chi biet FAIL/WARNING nen mot lan chay co checkpoint khong do duoc van ra
     * worst_status=PASS - dung dieu ma buildNoMeasurementReport phu nhan.
     */
    static String worseOf(String current, String candidate) {
        return rank(candidate) > rank(current) ? candidate : current;
    }

    private static int rank(String status) {
        if (status == null) return 0;
        return switch (status) {
            case "FAIL" -> 4;
            case "NO_RULES_APPLIED" -> 3;
            case "INSUFFICIENT_DATA" -> 3;
            case "WARNING" -> 2;
            default -> 1;                 // PASS va moi status chua biet
        };
    }

    /**
     * Report cho mot lan chay KHONG DO DUOC GI.
     *
     * Vi sao phai co: truoc day DqGuard.afterWrite gap truong hop nay thi log mot dong roi
     * return, khong ghi gi ca. Hau qua da xay ra that 21/09/2026: mot lan chay do 0 rule
     * (Gen Rules cham tran vong lap nen active.json khong duoc ghi) de nguyen
     * /dq/reports/{key}/latest.json cua lan chay TRUOC - mot tai lieu day du 9 stage va diem
     * so, tu 15 gio truoc. Ai doc latest.json khong co cach nao biet lan chay gan nhat khong
     * do gi. Im lang o day la kieu sai nguy hiem nhat: no trong y het thanh cong.
     *
     * worst_status = NO_RULES_APPLIED, KHONG BAO GIO la PASS. Khong do duoc khong phai la dat.
     *
     * @param declaredStages ten cac checkpoint dev da dat, lay tu manifest - de nguoi doc biet
     *                       le ra phai co gi o day.
     */
    public static ObjectNode buildNoMeasurementReport(
            String datasetName,
            String appId,
            DqRuleSet ruleSet,
            List<String> declaredStages,
            String reason) {

        ObjectNode doc = MAPPER.createObjectNode();
        doc.put("dataset_name", datasetName);
        doc.put("run_id", appId);
        doc.put("executed_at", Instant.now().toString());
        doc.put("checked_against_generation_run_id", ruleSet.generationRunId());
        doc.put("report_shape", "no_measurement");
        doc.putArray("stages");
        doc.put("stage_count", 0);
        doc.put("worst_status", "NO_RULES_APPLIED");
        doc.put("rules_declared", 0);
        doc.put("rules_attached", 0);
        doc.put("no_measurement_reason", reason);
        ArrayNode d = doc.putArray("declared_stages");
        for (String s : declaredStages) d.add(s);
        return doc;
    }

    public static ObjectNode buildReport(
            String datasetName,
            String appId,
            DqRuleSet ruleSet,
            List<DqRule> attachedRules,
            Map<String, Object> observedMetrics) {

        ObjectNode report = MAPPER.createObjectNode();
        report.put("dataset_name", datasetName);
        report.put("run_id", appId);
        report.put("executed_at", Instant.now().toString());
        report.put("checked_against_generation_run_id", ruleSet.generationRunId());

        long totalRows = ((Number) observedMetrics.getOrDefault(
                DqMetricQuery.TOTAL_ROWS_METRIC, 0L)).longValue();
        report.put("total_rows", totalRows);

        ArrayNode ruleResults = report.putArray("rules");
        double weightedErrorSum = 0.0;
        int weightSum = 0;
        // Diem thu hai, tinh khi da bo cac rule dang ngo. Xem DqConfig.SUSPECT_RULE_PCT.
        double trustedErrorSum = 0.0;
        int trustedWeightSum = 0;
        ArrayNode suspectRules = MAPPER.createArrayNode();

        for (DqRule rule : attachedRules) {
            long raw = ((Number) observedMetrics.getOrDefault(rule.metricName(), 0L)).longValue();

            // Rule muc TAP khong co ty le dong: metric la co 1/0. Quy doi NHI PHAN sang thang
            // phan tram de dung chung mot cong thuc diem voi rule muc dong - vi pham thi coi nhu
            // hong toan bo, vi mot khoa khong duy nhat la hong ca bang chu khong hong vai dong.
            boolean datasetLevel = rule.isDatasetLevel();
            Object observedRaw = rule.hasObservedExpr()
                    ? observedMetrics.get(rule.observedMetricName()) : null;

            // Rule muc tap khong co ty le dong SAN, nhung thuong suy ra duoc tu gia tri quan
            // sat neu rule khai don vi: 9 dong trung khoa tren 480 la 1.88%, khong phai 100%.
            // Chi khi khong suy duoc (avg, max...) moi dung quy uoc nhi phan - luc do "vi pham"
            // nghia la hong toan bo, vi mot khoa khong duy nhat la hong ca bang.
            Double derivedPct = null;
            if (datasetLevel && raw != 0 && observedRaw instanceof Number n) {
                derivedPct = rule.violationPctFrom(n.doubleValue(), totalRows);
            }
            double violationPct = datasetLevel
                    ? (raw != 0 ? (derivedPct != null ? derivedPct : 100.0) : 0.0)
                    : (totalRows > 0 ? (raw * 100.0 / totalRows) : 0.0);
            long violationCount = datasetLevel
                    ? Math.round(violationPct * totalRows / 100.0)
                    : raw;

            ObjectNode rr = ruleResults.addObject();
            rr.put("rule_id", rule.ruleId());
            rr.put("column", rule.column());
            rr.put("kind", rule.kind());
            rr.put("importance_weight", rule.importanceWeight());
            rr.put("violation_count", violationCount);
            rr.put("total_rows", totalRows);
            rr.put("violation_pct", Math.round(violationPct * 100.0) / 100.0);
            rr.put("observed_unit", rule.observedUnit());
            rr.put("pct_derived_from_observed", derivedPct != null);
            if (datasetLevel) rr.put("observed_flag", raw);
            // Rule muc tap: con so that va nguong ky vong, de doc report khong phai mo rule
            // set ra tra. Voi rule muc dong thi khong can - "0 dong vi pham" la ky vong hien
            // nhien, va violation_count da chinh la con so that.
            if (rule.hasObservedExpr()) {
                rr.put("observed_expr", rule.observedExpr());
                if (observedRaw instanceof Number) {
                    rr.put("observed_value", ((Number) observedRaw).doubleValue());
                } else if (observedRaw != null) {
                    rr.put("observed_value", String.valueOf(observedRaw));
                }
            }
            if (rule.expected() != null && !rule.expected().isBlank()) {
                rr.put("expected", rule.expected());
            }

            // Chi nghi ngo khi co du lieu that de nghi: tren bang rong thi moi ty le deu vo nghia.
            // Mien tru duy nhat la rule muc tap dung QUY UOC nhi phan - 100% o do la cach ghi
            // "co vi pham", khong phai mot phep do. Con khi ty le suy duoc tu gia tri that thi
            // no la phep do nhu moi rule khac, va 100% van la dau hieu rule sai.
            boolean binaryFallback = datasetLevel && derivedPct == null;
            boolean suspect = totalRows > 0 && !binaryFallback
                    && violationPct >= DqConfig.SUSPECT_RULE_PCT;
            rr.put("suspect_always_violating", suspect);
            if (suspect) {
                suspectRules.add(rule.ruleId());
            } else {
                trustedErrorSum += violationPct * rule.importanceWeight();
                trustedWeightSum += rule.importanceWeight();
            }

            weightedErrorSum += violationPct * rule.importanceWeight();
            weightSum += rule.importanceWeight();
        }
        report.set("suspect_rules", suspectRules);

        if (totalRows == 0) {
            report.put("status", "INSUFFICIENT_DATA");
            report.put("quality_score", (Double) null);
        } else if (weightSum == 0) {
            report.put("status", "NO_RULES_APPLIED");
            report.put("quality_score", 1.0);
        } else {
            double weightedError = weightedErrorSum / weightSum;
            double qualityScore = 1.0 - (weightedError / 100.0);
            report.put("quality_score", Math.round(qualityScore * 10000.0) / 10000.0);

            // Trang thai lay theo diem DA BO rule dang ngo: mot rule viet sai khong duoc mot
            // minh keo ca checkpoint xuong FAIL. Nhung su ton tai cua no thi phai nhin thay -
            // nen san them WARNING, va rule_health noi ro vi sao.
            double trustedScore = trustedWeightSum > 0
                    ? 1.0 - (trustedErrorSum / trustedWeightSum / 100.0) : qualityScore;
            report.put("quality_score_trusted", Math.round(trustedScore * 10000.0) / 10000.0);

            // Nguong ghi NGAY TRONG report cua tung chang, khong chi o cap tai lieu. Day la
            // thu duy nhat agent nhin thay o mot gate: ctx chi mang stage_report, nen thieu
            // no thi agent chi noi duoc "FAIL" ma khong bao gio giai thich duoc vi sao, va
            // nguoi doc Agent Log thay "0.998 (FAIL)" canh mot nguong 0.9 ma ho biet -
            // khong cach nao doi chieu. Da xay ra that 21/09/2026.
            ObjectNode th = report.putObject("thresholds");
            th.put("fail", DqConfig.QUALITY_FAIL_THRESHOLD);
            th.put("warning", DqConfig.QUALITY_WARNING_THRESHOLD);
            th.put("suspect_rule_pct", DqConfig.SUSPECT_RULE_PCT);

            String status = trustedScore < DqConfig.QUALITY_FAIL_THRESHOLD ? "FAIL"
                    : trustedScore < DqConfig.QUALITY_WARNING_THRESHOLD ? "WARNING" : "PASS";
            if (!suspectRules.isEmpty() && "PASS".equals(status)) status = "WARNING";
            report.put("status", status);
            report.put("rule_health", suspectRules.isEmpty() ? "OK"
                    : suspectRules.size() + " rule vi pham >= " + DqConfig.SUSPECT_RULE_PCT
                      + "% - gan chac la rule viet sai, khong phai du lieu hong. Diem/trang thai"
                      + " o day tinh theo quality_score_trusted (da bo chung).");
        }

        return report;
    }

    public static void publish(Configuration hadoopConf, String hdfsBaseUri, String dqKey,
                               ObjectNode report, String appId) {
        String latestPath = hdfsBaseUri + "/dq/reports/" + dqKey + "/latest.json";
        String historyPath = hdfsBaseUri + "/dq/reports/" + dqKey + "/history/" + appId + ".json";

        // Ghi ca hai duong: latest.json la gia tri hien tai, history/ la ban ghi bat bien -
        // mot lan sinh rule toi co the rollback bang cach copy ban history de len active.json.
        byte[] bytes = DqJson.bytes(report.toPrettyString());
        boolean ok = DqJson.write(hadoopConf, hdfsBaseUri, latestPath, bytes)
                & DqJson.write(hadoopConf, hdfsBaseUri, historyPath, bytes);
        if (ok) {
            LOG.info("=== DqReportWriter: da publish report -> {}", latestPath);
        } else {
            LOG.warn("=== DqReportWriter: khong publish duoc day du report len HDFS (bo qua)");
        }
    }
}
