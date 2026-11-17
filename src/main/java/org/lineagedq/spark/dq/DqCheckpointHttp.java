package org.lineagedq.spark.dq;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;

/**
 * Goi dich vu agent qua HTTP, thay cho viec fork mot tien trinh Python trong chinh driver pod.
 *
 * JAVA VAN LA BEN GOI. Driver KHONG mo cong nghe nao. Do la ly do phuong an nay duoc chon thay
 * vi de agent goi nguoc vao driver: khong job nao co System.exit, JVM driver chi thoat khi het
 * thread non-daemon, nen mot thread nghe sot lai la pod o mai Running, SparkApplication khong
 * bao gio COMPLETED, va task Airflow treo. Nguoc lai, moi thread cua java.net.http deu la
 * daemon (da do that tren JDK 17: HttpClient-1-SelectorManager va cac Worker deu daemon=true,
 * JVM thoat sau ~365ms), nen duong nay khong them duoc mot duong treo nao.
 *
 * Vong lap o day la anh xa 1-1 cua DqCheckpoint.runProtocol, chi khac ong:
 *     mo phien            <- thay dong "hello" + file --context-file + env DQ_KEY/DQ_DATASET_NAME
 *     long-poll /next     <- thay viec doc stdout cua tien trinh con
 *     POST .../result     <- thay viec ghi tool_response vao stdin
 *     DELETE phien        <- khong co doi ung cu; can vi ben kia khong con la con cua ta
 *
 * BA DIEU BAT BUOC:
 *   1. DEADLINE TUYET DOI. Tinh mot lan luc vao, moi request timeout <= thoi gian con lai. Cong
 *      timeout tung chang lai thi tong co the vuot xa ngan sach ma Java da hua voi pipeline.
 *   2. DONG REGISTRY TRUOC KHI BO CUOC. Sau khi Java tra PROCEED, SparkSession di tiep; mot
 *      tool_result ve muon ma van chay spark.sql() la cham vao trang thai da doi.
 *   3. MOI DUONG THAT BAI RA PROCEED, va de lai dau vet. Mot checkpoint hong khong duoc trong
 *      giong het mot checkpoint PROCEED that.
 */
final class DqCheckpointHttp {

    private static final Logger LOG = LoggerFactory.getLogger(DqCheckpointHttp.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    /** Ket qua mot luot goi HTTP: co decision, hay that bai va vi sao. */
    record Outcome(DqCheckpoint.Result result, String failure, int httpStatus) {
        boolean ok() {
            return result != null;
        }
    }

    private DqCheckpointHttp() {
    }

    /**
     * Chay tron mot checkpoint qua dich vu. Tra Outcome.ok()=false khi KHONG lay duoc quyet dinh
     * (luc do caller quyet dinh co lui ve ProcessBuilder khong).
     *
     * KHONG nem exception ra ngoai.
     */
    static Outcome call(String checkpoint, ObjectNode context, DqToolRegistry tools,
                        long budgetMs, boolean closeToolsOnExit) {
        final long deadlineAt = System.currentTimeMillis() + budgetMs;
        // Chua lai mot khoang o cuoi: het sach ngan sach roi moi bo cuoc thi khong con thoi gian
        // de DELETE phien, va dich vu se giu executor cua no them mot luc vo ich.
        final long bailAt = deadlineAt - DqConfig.HTTP_RESERVE_MS;
        String sessionId = null;
        HttpClient http = null;
        try {
            http = HttpClient.newBuilder()
                    .connectTimeout(Duration.ofMillis(DqConfig.HTTP_CONNECT_MS))
                    .version(HttpClient.Version.HTTP_1_1)
                    .build();

            ObjectNode open = openBody(checkpoint, context, tools, budgetMs);
            HttpResponse<String> r = send(http, "POST", "/v1/sessions", open,
                    Math.min(DqConfig.HTTP_OPEN_MS, left(bailAt)));
            if (r.statusCode() != 200 && r.statusCode() != 201) {
                return new Outcome(null, "mo phien that bai: HTTP " + r.statusCode()
                        + " " + shorten(r.body()), r.statusCode());
            }
            JsonNode ses = MAPPER.readTree(r.body());
            sessionId = ses.path("session_id").asText(null);
            if (sessionId == null || sessionId.isBlank()) {
                return new Outcome(null, "dich vu khong tra session_id", r.statusCode());
            }
            LOG.info("=== [checkpoint {}] phien {} mo tren {} (hang doi {}, ngan sach {}ms)",
                    checkpoint, shortId(sessionId), DqConfig.AGENT_API_URL,
                    ses.path("queue_position").asInt(0), budgetMs);

            return poll(http, checkpoint, sessionId, tools, bailAt);

        } catch (Exception e) {
            return new Outcome(null, "loi khi goi dich vu agent: " + e, 0);
        } finally {
            // DONG REGISTRY TRUOC, roi moi DELETE. Thu tu nay quan trong: sau khi caller tra
            // PROCEED, pipeline di tiep ngay, nen tool phai bi khoa TRUOC khi con bat ky duong
            // nao con co the goi no.
            //
            // closeToolsOnExit=false khi caller con dinh lui ve tien trinh con: luc do registry
            // phai con song, va CALLER chiu trach nhiem dong no truoc khi tra ve. Co y khong lam
            // "dong roi mo lai" - mot API nhu vay se hua hen hoi phuc duoc ca tool that su co
            // tai nguyen, ma no khong lam duoc.
            if (closeToolsOnExit && tools != null) tools.close();
            if (sessionId != null && http != null) deleteQuietly(http, sessionId);
        }
    }

    /** Vong long-poll. Anh xa cua vong while(readLine()) trong runProtocol. */
    private static Outcome poll(HttpClient http, String checkpoint, String sessionId,
                                DqToolRegistry tools, long bailAt) {
        long ackSeq = 0;
        int polls = 0;
        int noProgress = 0;
        int transient_ = 0;

        while (left(bailAt) > 0) {
            if (++polls > DqConfig.HTTP_MAX_POLLS) {
                return new Outcome(null, "vuot " + DqConfig.HTTP_MAX_POLLS + " lan poll", 0);
            }
            long waitMs = Math.max(0, Math.min(DqConfig.HTTP_POLL_WAIT_MS, left(bailAt)));
            HttpResponse<String> r;
            try {
                r = send(http, "GET",
                        "/v1/sessions/" + sessionId + "/next?ack_seq=" + ackSeq
                                + "&wait_ms=" + waitMs,
                        null, waitMs + DqConfig.HTTP_POLL_SLACK_MS);
            } catch (Exception e) {
                // Mang chap chon: thu lai vai lan roi moi bo. Phien van con ben kia, ackSeq giu
                // nguyen nen khong mat item nao.
                if (++transient_ > DqConfig.HTTP_MAX_TRANSIENT_RETRY) {
                    return new Outcome(null, "mat ket noi toi dich vu: " + e, 0);
                }
                LOG.warn("=== [checkpoint {}] poll loi ({}), thu lai {}/{}",
                        checkpoint, e, transient_, DqConfig.HTTP_MAX_TRANSIENT_RETRY);
                continue;
            }
            transient_ = 0;

            if (r.statusCode() == 404) {
                // Phien bien mat. last_decision != null la duong cuu: dich vu da co quyet dinh
                // roi, chi la goi tin truoc do mat.
                try {
                    JsonNode gone = MAPPER.readTree(r.body());
                    JsonNode last = gone.path("last_decision");
                    if (last != null && !last.isNull() && last.hasNonNull("action")) {
                        return new Outcome(toResult(last), null, 404);
                    }
                    return new Outcome(null, "phien bien mat (" + gone.path("reason").asText("?")
                            + ") va khong co quyet dinh nao", 404);
                } catch (Exception e) {
                    return new Outcome(null, "phien bien mat, than 404 khong doc duoc: " + e, 404);
                }
            }
            if (r.statusCode() != 200) {
                return new Outcome(null, "poll tra HTTP " + r.statusCode() + " "
                        + shorten(r.body()), r.statusCode());
            }

            JsonNode item;
            try {
                item = MAPPER.readTree(r.body());
            } catch (Exception e) {
                return new Outcome(null, "than poll khong phai JSON: " + e, 200);
            }
            String type = item.path("type").asText("");

            if ("idle".equals(type)) {
                // Khong co viec. wait_for_item ben kia da GIU request du lau nen day khong phai
                // busy-loop; nhung neu dich vu tra idle tuc thi mai (vi du loi cau hinh) thi
                // dem nay chan lai.
                if (item.path("held_ms").asLong(0) < 100 && ++noProgress > DqConfig.HTTP_MAX_NO_PROGRESS) {
                    return new Outcome(null, "dich vu tra idle lien tuc ma khong giu request", 200);
                }
                continue;
            }
            noProgress = 0;

            if ("tool_request".equals(type)) {
                ackSeq = item.path("seq").asLong(ackSeq);
                String callId = item.path("call_id").asText("");
                String tool = item.path("tool").asText("");
                ObjectNode resp = tools == null
                        ? (ObjectNode) MAPPER.createObjectNode().put("type", "tool_response")
                            .put("ok", false).put("error", "Job khong mo kenh live cho checkpoint nay.")
                        : tools.handle(tool, item.path("args"));
                resp.put("protocol_version", 1);
                try {
                    HttpResponse<String> pr = send(http, "POST",
                            "/v1/sessions/" + sessionId + "/calls/" + callId + "/result", resp,
                            Math.min(DqConfig.HTTP_RESULT_MS, left(bailAt)));
                    if (pr.statusCode() != 200) {
                        LOG.warn("=== [checkpoint {}] tra ket qua tool '{}' bi tu choi: HTTP {} {}",
                                checkpoint, tool, pr.statusCode(), shorten(pr.body()));
                    }
                } catch (Exception e) {
                    return new Outcome(null, "khong tra duoc ket qua tool: " + e, 0);
                }
                continue;
            }

            if ("decision".equals(type)) {
                ackSeq = item.path("seq").asLong(ackSeq);
                if (item.path("synthetic").asBoolean(false)) {
                    // Dich vu tu dung quyet dinh nay (agent khong tra ve gi). Van la PROCEED,
                    // nhung phai noi ro trong log - no KHONG phai y kien cua agent.
                    LOG.warn("=== [checkpoint {}] dich vu tu dung quyet dinh (agent khong tra loi): {}",
                            checkpoint, item.path("reason").asText(""));
                }
                return new Outcome(toResult(item), null, 200);
            }

            if ("gone".equals(type)) {
                JsonNode last = item.path("last_decision");
                if (last != null && !last.isNull() && last.hasNonNull("action")) {
                    return new Outcome(toResult(last), null, 200);
                }
                return new Outcome(null, "phien ket thuc khong co quyet dinh", 200);
            }
            LOG.warn("=== [checkpoint {}] bo qua item la type={}", checkpoint, type);
        }
        return new Outcome(null, "het ngan sach khi cho dich vu agent", 0);
    }

    // ── tien ich ────────────────────────────────────────────────────────

    private static ObjectNode openBody(String checkpoint, ObjectNode context,
                                       DqToolRegistry tools, long budgetMs) {
        ObjectNode b = MAPPER.createObjectNode();
        b.put("protocol_version", 1);
        String runId = context.path("spark_app_id").asText("");
        // request_id la KHOA CHONG CHAY LAI ben dich vu: mot goi tin 201 mat khong duoc bien
        // thanh hai lan chay agent, hai lan tien LLM, va co the hai quyet dinh khac nhau.
        b.put("request_id", runId + ":" + checkpoint);
        b.put("checkpoint", checkpoint);
        b.put("dq_key", context.path("dq_key").asText(""));
        b.put("dataset_name", context.path("dataset_name").asText(""));
        b.put("run_id", runId);
        b.put("budget_ms", budgetMs);
        // Tuy chon: chi gui khi job dat nguong rieng. Khong gui thi dich vu dung mac dinh.
        if (DqConfig.AGENT_MAX_ITERATIONS > 0) {
            b.put("llm_max_iterations", DqConfig.AGENT_MAX_ITERATIONS);
        }
        b.set("context", context);
        b.set("tools", tools == null || tools.isEmpty()
                ? MAPPER.createArrayNode() : tools.declarations());
        return b;
    }

    private static DqCheckpoint.Result toResult(JsonNode decision) {
        DqCheckpoint.Action action =
                DqCheckpoint.Action.parse(decision.path("action").asText("PROCEED"));
        return new DqCheckpoint.Result(action, decision.path("reason").asText(""), decision);
    }

    private static HttpResponse<String> send(HttpClient http, String method, String path,
                                             ObjectNode body, long timeoutMs) throws Exception {
        HttpRequest.Builder b = HttpRequest.newBuilder()
                .uri(URI.create(DqConfig.AGENT_API_URL + path))
                // Toi thieu 1ms: Duration.ZERO lam HttpRequest nem IllegalArgumentException, va
                // luc het ngan sach thi left() rat de ra 0.
                .timeout(Duration.ofMillis(Math.max(1, timeoutMs)))
                .header("Content-Type", "application/json");
        if (!DqConfig.AGENT_API_TOKEN.isBlank()) {
            b.header("Authorization", "Bearer " + DqConfig.AGENT_API_TOKEN);
        }
        if (body == null) {
            b.method(method, HttpRequest.BodyPublishers.noBody());
        } else {
            b.method(method, HttpRequest.BodyPublishers.ofString(
                    MAPPER.writeValueAsString(body), StandardCharsets.UTF_8));
        }
        return http.send(b.build(), HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
    }

    /** Bao dich vu don phien. Tot nhat co gang - hong thi thoi, phien tu het han. */
    private static void deleteQuietly(HttpClient http, String sessionId) {
        try {
            send(http, "DELETE", "/v1/sessions/" + sessionId, null, DqConfig.HTTP_DELETE_MS);
        } catch (Exception e) {
            LOG.debug("=== khong DELETE duoc phien {} (bo qua): {}", shortId(sessionId), e.toString());
        }
    }

    private static long left(long bailAt) {
        return bailAt - System.currentTimeMillis();
    }

    private static String shortId(String id) {
        return id == null ? "?" : id.substring(0, Math.min(8, id.length()));
    }

    private static String shorten(String s) {
        if (s == null) return "";
        return s.length() <= 300 ? s : s.substring(0, 300) + "...";
    }
}
