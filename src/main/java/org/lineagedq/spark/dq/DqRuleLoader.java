package org.lineagedq.spark.dq;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Doc bo rule DQ dang active tu HDFS - khu vuc /dq/ tach biet hoan toan khoi /warehouse (du lieu
 * pipeline) va /jars (binary), theo yeu cau tach DQ thanh service rieng trong tuong lai.
 *
 * Rule do Gen Rules agent publish len /dq/rules/{key}/active.json.
 *
 * Best-effort: khong tim thay rule (chua Gen Rules lan nao, hoac loi mang) tra ve DqRuleSet.empty()
 * thay vi nem exception - kiem tra DQ la metadata phu tro, khong duoc lam hong job chinh.
 */
public final class DqRuleLoader {

    private static final Logger LOG = LoggerFactory.getLogger(DqRuleLoader.class);

    private DqRuleLoader() {
    }

    /**
     * @param hdfsBaseUri vd "hdfs://namenode:9000"
     * @param dqKey       ten bang ngan gon dung lam key thu muc, vd ten bang dau ra
     */
    /** Verdict cua khau verify cho mot generation_run_id. */
    public static String verificationPath(String hdfsBaseUri, String dqKey, String generationRunId) {
        return hdfsBaseUri + "/dq/rules/" + dqKey + "/verification/" + generationRunId + ".json";
    }

    /**
     * Chot cung o phia Java: rule set KHONG co verdict khop generation_run_id thi KHONG duoc nap.
     *
     * Day la cho duy nhat bien "verify tu choi" thanh co hieu luc. O tang Python, verifier giu
     * doc quyen tool promote_rules chi la QUY UOC - moi agent chay cung tien trinh deu
     * `from dq_store import publish_rules` duoc.
     *
     * DQ_RULES_LEGACY_AMNESTY=true de chay dung mot luot chuyen he: active.json dang co tren cum
     * sinh truoc khi co khau verify nen khong co verdict nao ca.
     */
    /**
     * Doc phan quyet tham dinh cua mot bo rule. null khi chua co.
     *
     * Cong khai vi DqWarehouseWriter can no de mirror per_stage sang bang - verdict tong da
     * co trong JSON nhung diem tung checkpoint thi chua tra duoc bang SQL.
     */
    public static JsonNode readVerdict(Configuration hadoopConf, String hdfsBaseUri, String dqKey,
                                       DqRuleSet ruleSet) {
        if (ruleSet == null || ruleSet.generationRunId() == null) return null;
        try {
            return DqJson.read(hadoopConf, hdfsBaseUri,
                    verificationPath(hdfsBaseUri, dqKey, ruleSet.generationRunId()));
        } catch (Exception e) {
            LOG.warn("=== DqRuleLoader: khong doc duoc verdict: {}", e.toString());
            return null;
        }
    }

    private static boolean verified(Configuration hadoopConf, String hdfsBaseUri, String dqKey,
                                    DqRuleSet ruleSet) {
        String gen = ruleSet.generationRunId();
        if (gen == null || gen.isBlank()) {
            LOG.error("=== DqRuleLoader: rule set khong co generation_run_id - khong xac minh duoc.");
            return false;
        }
        String path = verificationPath(hdfsBaseUri, dqKey, gen);
        try {
            JsonNode v = DqJson.read(hadoopConf, hdfsBaseUri, path);
            if (v == null) {
                LOG.error("=== DqRuleLoader: KHONG co verdict tai {} - bo rule nay CHUA duoc tham"
                        + " dinh, tu choi nap.", path);
                return false;
            }
            String verdict = v.path("verdict").asText("");
            boolean ok = "PASS".equals(verdict) || "WEAK".equals(verdict);
            if (!ok) {
                LOG.error("=== DqRuleLoader: verdict={} cho generation_run_id={} - tu choi nap.",
                        verdict, gen);
            }
            return ok;
        } catch (Exception e) {
            LOG.error("=== DqRuleLoader: khong doc duoc verdict {} ({}) - tu choi nap.", path, e.toString());
            return false;
        }
    }

    public static DqRuleSet loadActive(Configuration hadoopConf, String hdfsBaseUri, String dqKey) {
        String path = hdfsBaseUri + "/dq/rules/" + dqKey + "/active.json";
        LOG.info("=== DqRuleLoader: doc rule tu {}", path);

        try {
            JsonNode root = DqJson.read(hadoopConf, hdfsBaseUri, path);
            if (root == null) {
                LOG.warn("=== DqRuleLoader: chua co rule nao tai {} (Gen Rules chua chay lan nao?) - bo qua DQ check.", path);
                return DqRuleSet.empty();
            }

            List<DqRule> rules = new ArrayList<>();
            for (JsonNode r : root.path("rules")) {
                rules.add(new DqRule(
                        r.path("stage").asText(DqRule.STAGE_FINAL),
                        r.path("rule_id").asText(""),
                        r.path("column").asText(""),
                        r.path("description").asText(""),
                        r.path("failed_sql_expr").asText(""),
                        r.path("importance_weight").asInt(1),
                        r.path("kind").asText(DqRule.KIND_ROW),
                        // Hai truong TUY CHON, chi rule muc tap moi can: gia tri that va nguong
                        // ky vong. Thieu thi report chi co dung/sai - biet la hong nhung khong
                        // biet hong bao nhieu.
                        r.path("observed_expr").asText(null),
                        r.path("expected").asText(null),
                        // Don vi cua observed_expr - quyet dinh co quy doi ra ty le duoc khong.
                        // Thieu thi giu quy uoc nhi phan cu, nen rule cu khong hong.
                        r.path("observed_unit").asText(DqRule.UNIT_VALUE)));
            }

            DqRuleSet ruleSet = new DqRuleSet(
                    root.path("dataset_name").asText(null),
                    root.path("generation_run_id").asText(null),
                    root.path("generated_at").asText(null),
                    root.path("lineage_hash").asText(null),
                    rules,
                    root.path("policy"));

            LOG.info("=== DqRuleLoader: doc duoc {} rule qua {} stage {} (generation_run_id={})",
                    rules.size(), ruleSet.stages().size(), ruleSet.stages(), ruleSet.generationRunId());

            if (!verified(hadoopConf, hdfsBaseUri, dqKey, ruleSet)) {
                if (!DqConfig.RULES_LEGACY_AMNESTY) {
                    LOG.error("=== DqRuleLoader: CHAY VOI 0 RULE. Dat DQ_RULES_LEGACY_AMNESTY=true"
                            + " dung mot luot neu day la bo rule cu tu truoc khi co khau verify.");
                    return DqRuleSet.empty();
                }
                LOG.warn("=== DqRuleLoader: rule set chua tham dinh nhung DQ_RULES_LEGACY_AMNESTY=true"
                        + " -> van nap. Hay TAT co nay sau luot chuyen he.");
            }
            return ruleSet;
        } catch (Exception e) {
            LOG.warn("=== DqRuleLoader: loi doc rule tu {} (bo qua DQ check): {}", path, e.toString());
            return DqRuleSet.empty();
        }
    }
}
