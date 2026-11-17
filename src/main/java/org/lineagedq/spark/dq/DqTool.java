package org.lineagedq.spark.dq;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * Mot kha nang job Spark mo ra cho agent trong luc job dang chay.
 *
 * Checkpoint khong phai kich ban co dinh: job cong bo nhung gi no cho phep (qua dong "hello"
 * dau kenh), agent tu chon goi cai nao. Them kha nang = them mot lop implement interface nay
 * va dang ky vao DqToolRegistry, khong sua DqCheckpoint va khong sua code Python.
 */
public interface DqTool {

    /** Ten agent goi. Phai la dinh danh hop le trong schema tool cua LLM. */
    String name();

    /** Mo ta di thang vao prompt cua agent - do la tat ca thong tin no co de quyet dinh. */
    String description();

    /** JSON Schema cua tham so: {"type":"object","properties":{...},"required":[...]}. */
    ObjectNode parameters();

    /**
     * @return noi dung truong "result" cua tool_response
     * @throws Exception DqToolRegistry bien thanh {"ok":false,"error":...} gui ve cho agent tu
     *         sua, thay vi lam hong ca checkpoint
     */
    JsonNode execute(JsonNode args) throws Exception;
}
