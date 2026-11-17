package org.lineagedq.spark.dq;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Cho nguoi quyet dinh tai mot checkpoint: doc lap ban ghi quyet dinh tren HDFS cho toi khi
 * co cau tra loi.
 *
 * <h2>Vi sao vong cho nam o JAVA chu khong nam trong tool cua agent</h2>
 *
 * Cach hien nhien la de tool {@code request_human_decision} cua Orchestrator tu chan cho toi
 * khi co nguoi tra loi. Cach do HONG, vi mot ly do rat cu the: duong HTTP co chot
 * {@code DQ_AGENT_HTTP_MAX_NO_PROGRESS = 200} lan poll x {@code HTTP_POLL_WAIT_MS = 20s},
 * tuc khoang 66 phut KHONG CO TIEN TRIEN la Java bo cuoc va tra PROCEED. Agent dang nam cho
 * nguoi thi khong goi tool nao ca - dung dinh nghia "khong co tien trien". Nen phien se chet
 * truoc khi nguoi kip tra loi, va chet theo kieu te nhat: tra PROCEED, tuc GHI DU LIEU RAC,
 * dung dieu framework nay sinh ra de tranh.
 *
 * Nen chia vai:
 * <pre>
 *   Orchestrator  ghi ban ghi "cho quyet dinh" roi TRA VE NGAY,
 *                 ket thuc bang checkpoint_decision("AWAIT_HUMAN", ...)
 *   Java          nhan AWAIT_HUMAN -> dong phien agent -> tu doc lap ban ghi tren HDFS
 * </pre>
 *
 * Phien agent song ngan va ket thuc sach; vong cho dai nam o noi that su can cho. Doc lap
 * bang HDFS chu khong giu ket noi nao, nen khong co gi de het han hay dut giua chung.
 *
 * <h2>Khong co han muc thoi gian</h2>
 *
 * Co y, theo yeu cau: nguoi quyet dinh, khong phai dong ho. Dat {@code DQ_HUMAN_WAIT_MAX_SEC}
 * lon hon 0 de bat han muc; khi do het gio se ap {@code default_on_timeout} ghi trong chinh
 * ban ghi quyet dinh - va truong do BAT BUOC khai, khong co gia tri ngam, vi "khong ai tra
 * loi" la tinh huong thuong gap chu khong phai ngoai le.
 */
final class DqHumanDecision {

    private static final Logger LOG = LoggerFactory.getLogger(DqHumanDecision.class);

    static final String STATUS_PENDING = "pending";

    private DqHumanDecision() {
    }

    /** Duong dan ban ghi quyet dinh cua mot checkpoint trong mot lan chay. */
    static String path(String hdfsBaseUri, String dqKey, String runId, String stage) {
        return hdfsBaseUri + "/dq/decisions/" + dqKey + "/" + runId + "/" + stage + ".json";
    }

    /**
     * Cho toi khi ban ghi khong con {@code status=pending}.
     *
     * @return hanh dong cuoi cung. Khong doc duoc ban ghi, hoac het han muc ma van pending,
     *         thi ve {@code default_on_timeout}; ban ghi khong khai truong do thi ve HALT -
     *         mac dinh an toan cho muc tieu khong ghi rac.
     */
    static DqCheckpoint.Result await(Configuration hadoopConf, String hdfsBaseUri, String dqKey,
                                     String runId, String stage) {
        String file = path(hdfsBaseUri, dqKey, runId, stage);
        long startedAt = System.currentTimeMillis();
        long maxMs = DqConfig.HUMAN_WAIT_MAX_SEC > 0 ? DqConfig.HUMAN_WAIT_MAX_SEC * 1000L : 0L;

        LOG.error("=== [gate {}] CHO NGUOI QUYET DINH. Tra loi bang:\n"
                        + "    scripts/dq-decide.sh {} {} PROCEED|PROCEED_ALL|HALT_AT_WRITE|HALT"
                        + " --reason \"...\"\n"
                        + "    (ban ghi: {})",
                stage, runId, stage, file);

        JsonNode last = null;
        for (long attempt = 1; ; attempt++) {
            JsonNode doc = DqJson.read(hadoopConf, hdfsBaseUri, file);
            if (doc != null) {
                last = doc;
                String status = doc.path("status").asText(STATUS_PENDING);
                if (!STATUS_PENDING.equals(status)) {
                    DqCheckpoint.Action action = DqCheckpoint.Action.parse(status);
                    String who = doc.path("decided_by").asText("khong ro");
                    String why = doc.path("decided_reason").asText("");
                    LOG.warn("=== [gate {}] nguoi da quyet: {} (boi {}) - {}",
                            stage, action, who, why);
                    return new DqCheckpoint.Result(action,
                            "nguoi quyet dinh: " + action + " (boi " + who + ") " + why, doc);
                }
            }

            long waitedMs = System.currentTimeMillis() - startedAt;
            if (maxMs > 0 && waitedMs >= maxMs) {
                return timedOut(stage, last, waitedMs);
            }
            // Nhac lai dinh ky, khong phai moi vong: mot dong moi phut la du de ai doc log biet
            // job dang cho gi, va khong lam ngap log.
            if (attempt % DqConfig.HUMAN_WAIT_LOG_EVERY == 0) {
                LOG.error("=== [gate {}] van dang CHO NGUOI sau {}s. {}",
                        stage, waitedMs / 1000, file);
            }
            try {
                Thread.sleep(DqConfig.HUMAN_WAIT_POLL_MS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                LOG.warn("=== [gate {}] vong cho bi ngat - ap mac dinh an toan.", stage);
                return timedOut(stage, last, waitedMs);
            }
        }
    }

    /**
     * Het gio hoac bi ngat: ap default_on_timeout cua chinh ban ghi, khong doc duoc thi HALT.
     *
     * KHONG dung Action.parse o day. parse() tra PROCEED cho moi chuoi la - dung voi quyet
     * dinh cua agent (mot loi phan giai khong duoc bien thanh mot lan dung job), nhung NGUOC
     * o day: het gio ma khong biet phai lam gi thi phai nghieng ve an toan, khong phai ve
     * "cu ghi di". Hai bay cu the, ca hai deu ket thuc bang GHI du lieu ma khong ai dong y:
     *   - mot gia tri go sai, vd "PROCEDE" -> parse() ra PROCEED.
     *   - "AWAIT_HUMAN" -> parse() tra dung hang do, va checkpoint lai quay ve cho nguoi mot
     *     lan nua, tuc khong bao gio thoat duoc vong cho.
     * (Ca `"default_on_timeout": null` thi nhanh cu VAN dung: JsonNode.asText(null) tra ve
     * null that chu khong phai chuoi "null". Kiem lai roi, khong phai bay.)
     */
    private static DqCheckpoint.Result timedOut(String stage, JsonNode doc, long waitedMs) {
        String fallback = doc != null && doc.hasNonNull("default_on_timeout")
                ? doc.get("default_on_timeout").asText("").trim().toUpperCase() : "";
        DqCheckpoint.Action action = null;
        for (DqCheckpoint.Action candidate : DqCheckpoint.Action.values()) {
            if (candidate.name().equals(fallback) && candidate != DqCheckpoint.Action.AWAIT_HUMAN) {
                action = candidate;
            }
        }
        String reason = "khong ai tra loi sau " + waitedMs / 1000 + "s - ap "
                + (action == null
                   ? "mac dinh an toan HALT (default_on_timeout khong doc duoc: '"
                     + fallback + "')"
                   : "default_on_timeout=" + action);
        if (action == null) {
            action = DqCheckpoint.Action.HALT;
        }
        LOG.error("=== [gate {}] {}", stage, reason);
        return new DqCheckpoint.Result(action, reason, doc);
    }
}
