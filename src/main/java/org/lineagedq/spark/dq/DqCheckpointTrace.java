package org.lineagedq.spark.dq;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;

/**
 * Java tu ghi mot dong dau vet vao /dq/checkpoints/{dq_key}/{run_id}.json.
 *
 * VI SAO CAN: xua nay file nay do PYTHON ghi (dq_store.record_checkpoint). Khi dich vu agent
 * khong toi duoc thi khong co Python nao chay ca - khong ai ghi, va mot checkpoint PROCEED tro
 * thanh mau xanh im lang: job xanh, bang dq_agent_runs khong co dong nao, khong ai truy duoc
 * chuyen gi da xay ra. Lop nay lap dung khoang trong do.
 *
 * AI GHI - PHAN CONG TUYET DOI, khong chong nhau:
 *   - orchestrator.py : moi lan chay di den duoc doan ghi decision (khong co "recorded_by")
 *   - dich vu agent   : khi no TU CHE decision ("recorded_by":"agent-service")
 *   - Java (lop nay)  : CHI khi khong nhan duoc decision nao ("recorded_by":"java")
 * Luat khoa duy nhat: decision.recorded == true  =>  Java KHONG ghi.
 *
 * Dang tai lieu giu DUNG hinh ma dq_store.record_checkpoint (dq_store.py:90-107) tao ra va
 * DqWarehouseWriter.appendAgentRuns doc lai: {"dq_key","run_id","checkpoints":[entry,...]}.
 * DqWarehouseWriter chi lay checkpoint/action/reason/recorded_at + tool_calls.size(), nen ba
 * truong them (recorded_by/transport/outcome/http_status) KHONG doi schema dq_agent_runs.
 *
 * RUI RO da biet, noi thang: day la doc-sua-ghi khong nguyen tu va gio co hai nguoi ghi. Phan
 * cong o tren lam cua so tranh chap gan bang 0 (Java chi ghi khi chac chan dich vu khong ghi)
 * nhung khong phai 0 tuyet doi.
 */
public record DqCheckpointTrace(Configuration hadoopConf, String hdfsBaseUri,
                                String dqKey, String runId) {

    private static final Logger LOG = LoggerFactory.getLogger(DqCheckpointTrace.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    /**
     * DqGuard.Builder KHONG bat buoc hadoop(): mot pipeline co the dung DqGuard ma khong cau hinh
     * HDFS. Phai tu kiem truoc khi ghi thay vi tin vao nguoi tich hop.
     */
    public boolean usable() {
        return hadoopConf != null && hdfsBaseUri != null && dqKey != null && runId != null
                && !hdfsBaseUri.isBlank() && !dqKey.isBlank() && !runId.isBlank();
    }

    public String path() {
        return hdfsBaseUri + "/dq/checkpoints/" + dqKey + "/" + runId + ".json";
    }

    /**
     * Append mot entry. KHONG BAO GIO nem - boc ca than ham trong catch Throwable vi no chay o
     * dung cho de ton thuong nhat: sau khi checkpoint da het ngan sach, tren luong pipeline chinh.
     *
     * @param transport "http" hoac "process"
     * @param outcome   ly do may doc: service_unreachable | service_error | protocol_error |
     *                  session_lost | deadline_exceeded | both_paths_failed | decision_not_recorded
     * @param httpStatus ma HTTP neu co, 0 khi khong (loi transport, het ngan sach...)
     */
    public void record(String checkpoint, DqCheckpoint.Action action, String reason,
                       String transport, String outcome, int httpStatus) {
        if (!usable()) {
            LOG.warn("=== DqCheckpointTrace: khong co hadoopConf/hdfsBaseUri - KHONG ghi duoc dau"
                    + " vet cho checkpoint {} ({}). Duong PROCEED nay se khong co ban ghi nao.",
                    checkpoint, outcome);
            return;
        }
        try {
            String p = path();
            JsonNode existing = DqJson.read(hadoopConf, hdfsBaseUri, p);
            ObjectNode doc;
            ArrayNode entries;
            if (existing instanceof ObjectNode on && on.path("checkpoints").isArray()) {
                doc = on;
                entries = (ArrayNode) on.path("checkpoints");
            } else {
                doc = MAPPER.createObjectNode();
                doc.put("dq_key", dqKey);
                doc.put("run_id", runId);
                entries = doc.putArray("checkpoints");
            }

            ObjectNode entry = entries.addObject();
            entry.put("checkpoint", checkpoint);
            entry.put("action", action == null ? "PROCEED" : action.name());
            entry.put("reason", reason == null ? "" : reason);
            entry.put("recorded_at", Instant.now().toString());
            entry.put("live_channel", false);
            entry.put("recorded_by", "java");
            entry.put("transport", transport);
            entry.put("outcome", outcome);
            entry.put("http_status", httpStatus);

            boolean ok = DqJson.write(hadoopConf, hdfsBaseUri, p,
                    DqJson.bytes(doc.toPrettyString()));
            if (ok) {
                LOG.info("=== [checkpoint {}] da ghi dau vet ({}) -> {}", checkpoint, outcome, p);
            } else {
                LOG.warn("=== [checkpoint {}] KHONG ghi duoc dau vet ({}) -> {}",
                        checkpoint, outcome, p);
            }
        } catch (Throwable t) {  // dau vet khong bao gio duoc lam hong job
            LOG.warn("=== DqCheckpointTrace: ghi dau vet that bai cho checkpoint {} ({}): {}",
                    checkpoint, outcome, t.toString());
        }
    }
}
