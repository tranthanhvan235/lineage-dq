package org.lineagedq.spark.dq;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.Map;

/** Tap kha nang mot checkpoint mo ra cho agent. Them tool = add vao day, khong sua cho khac. */
public final class DqToolRegistry {

    private static final Logger LOG = LoggerFactory.getLogger(DqToolRegistry.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final Map<String, DqTool> tools = new LinkedHashMap<>();

    /**
     * volatile CO CHU Y: close() duoc goi tu luong pipeline chinh trong khi handle() co the dang
     * chay tren luong 'dq-checkpoint-{cp}'. Khong tin duoc vao mot minh viec ngat thread:
     * pool.shutdownNow() ngat duoc mot request HTTP dang block (da do: ~19ms), nhung neu cu ngat
     * roi dung vao GIUA mot spark.sql().collectAsList() thi tool van dang cam SparkSession. Mot
     * co 'dung lai' o cap registry la cach duy nhat de mot tool_result ve muon khong bao gio cham
     * vao SparkSession ma job da di tiep (DqGuard.afterWrite da unpersist cache).
     */
    private volatile boolean closed = false;

    public DqToolRegistry add(DqTool tool) {
        if (tool == null) return this;
        DqTool cu = tools.put(tool.name(), tool);
        if (cu != null) {
            LOG.warn("=== DqToolRegistry: tool '{}' bi dang ky de len ban truoc", tool.name());
        }
        return this;
    }

    public boolean isEmpty() {
        return tools.isEmpty();
    }

    /** Danh sach tool gui cho agent trong dong "hello", dung dang schema cua LLM tool-calling. */
    public ArrayNode declarations() {
        ArrayNode arr = MAPPER.createArrayNode();
        for (DqTool t : tools.values()) {
            ObjectNode n = arr.addObject();
            n.put("name", t.name());
            n.put("description", t.description());
            n.set("parameters", t.parameters());
        }
        return arr;
    }

    /**
     * Chay yeu cau cua agent, dong goi thanh tool_response. KHONG BAO GIO nem exception: loi
     * quay ve thanh {"ok":false,"error":...} de agent tu sua trong cung luot.
     */
    public ObjectNode handle(String name, JsonNode args) {
        ObjectNode resp = MAPPER.createObjectNode();
        resp.put("type", "tool_response");

        // Kiem TRUOC KHI cham vao tool: sau khi checkpoint ket thuc, du lieu trung gian da bi
        // unpersist va SparkSession da di tiep - tra loi cho agent con hon lam job hong.
        if (closed) {
            LOG.warn("=== DqToolRegistry: tu choi tool '{}' - registry da dong", name);
            return resp.put("ok", false).put("error",
                    "Checkpoint da ket thuc: job da di tiep, khong con du lieu trung gian de doc.");
        }

        DqTool tool = tools.get(name);
        if (tool == null) {
            return resp.put("ok", false).put("error",
                    "Tool khong ton tai o checkpoint nay: " + name
                            + ". Cac tool dang co: " + String.join(", ", tools.keySet()));
        }
        try {
            JsonNode result = tool.execute(args == null ? MAPPER.createObjectNode() : args);
            resp.put("ok", true);
            resp.set("result", result);
            return resp;
        } catch (Exception e) {  // xem javadoc
            LOG.warn("=== DqToolRegistry: tool '{}' loi: {}", name, e.toString());
            return resp.put("ok", false).put("error", String.valueOf(e.getMessage()));
        }
    }

    /**
     * Dong registry: moi loi goi handle() sau day tra loi thay vi cham vao tool.
     *
     * Idempotent va KHONG BAO GIO nem: no chay trong khoi finally cua checkpoint, ngay truoc khi
     * Java tra PROCEED. Tool nao la AutoCloseable thi duoc goi close() (vd DqLiveQuery huy
     * jobGroup Spark cua no) - loi cua mot tool khong duoc chan tool con lai.
     */
    public void close() {
        if (closed) return;
        closed = true;
        for (DqTool t : tools.values()) {
            if (t instanceof AutoCloseable c) {
                try {
                    c.close();
                } catch (Throwable ignored) {  // dong registry khong bao gio duoc lam hong job
                    LOG.debug("=== DqToolRegistry: close() cua tool loi (bo qua)");
                }
            }
        }
        LOG.debug("=== DqToolRegistry: da dong ({} tool)", tools.size());
    }

    public boolean isClosed() {
        return closed;
    }
}
