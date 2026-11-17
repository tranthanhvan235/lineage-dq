package org.lineagedq.spark.dq;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Goi multi-agent system (Python) tu trong job Spark dang chay.
 *
 * HAI DUONG, chon bang DQ_AGENT_MODE:
 *   http_then_process (MAC DINH)  goi dich vu agent qua HTTP; hong thi lui ve tien trinh con
 *   http_only                     chi HTTP; hong thi PROCEED kem dau vet
 *   process_only                  chi tien trinh con (hanh vi cu, giu de con duong lui)
 *
 * Duong HTTP o DqCheckpointHttp. Duong tien trinh con o callViaProcess ben duoi va GIU NGUYEN
 * khong sua - no la luoi an toan khi dich vu chet giua chung.
 *
 * Muc dich: mot lan spark-submit la co du CA pipeline LAN tuong tac agent - khong con can chay
 * orchestrator.py tu ngoai host roi moi submit job.
 *
 * Java LUON goi Orchestrator, khong bao gio goi thang subagent: dung nguyen tac "pipeline chi
 * noi chuyen voi Orchestrator", ben trong Orchestrator tu quyet dinh dispatch subagent nao.
 *
 * Kenh 2 CHIEU (khi truyen mot DqToolRegistry khong rong): Java dong vai TOOL SERVER cho agent.
 *   java  -> stdin : {"type":"hello","checkpoint":"...","tools":[{name,description,parameters}]}
 *   agent -> stdout: {"type":"tool_request","tool":"query_live_stage","args":{"sql":"..."}}
 *   java  -> stdin : {"type":"tool_response","ok":true,"result":{...}}
 *   agent -> stdout: {"type":"decision","action":"PROCEED","reason":"..."}
 *
 * Dong "hello" la diem mau chot: job cong bo nhung gi no cho phep, agent tu chon goi cai nao.
 * Them kha nang = them mot DqTool, khong sua file nay va khong sua code Python.
 *
 * Ba diem BAT BUOC:
 *   1. stderr -> FILE. Transcript agent rat dai; cho no qua pipe se day buffer (~64KB) va TREO
 *      JOB. stdout van la pipe nhung chi chua dong giao thuc, va Java doc lien tuc.
 *   2. Trao doi chay tren thread rieng co timeout - readLine() block vo han neu agent chet.
 *   3. MOI duong that bai tra PROCEED.
 */
public final class DqCheckpoint {

    private static final Logger LOG = LoggerFactory.getLogger(DqCheckpoint.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private DqCheckpoint() {
    }

    /**
     * Quyet dinh tai mot checkpoint.
     *
     * <pre>
     *   PROCEED        di tiep; checkpoint SAU van hoi neu lai hong
     *   PROCEED_ALL    di tiep va KHONG hoi nguoi nua trong ca luot chay nay
     *   HALT_AT_WRITE  chay tiep de chan doan het cac chang, nhung KHONG duoc ghi
     *   HALT           dung ngay tai day
     * </pre>
     *
     * PROCEED_ALL co vi cung mot nguyen nhan goc thuong lo ra o nhieu checkpoint: 15 dong
     * imsi_new hong o src_calls se lai hien ra thanh original_imsi o jn_map1. Hoi nguoi hai
     * lan ve cung mot chuyen la cach nhanh nhat de khong ai con doc canh bao nua.
     *
     * HALT_AT_WRITE co vi muc dich cua framework la KHONG GHI RAC, chu khong phai dung job.
     * Dung o chang 1 thi mat luon co hoi biet du lieu hong lan toi dau.
     */
    public enum Action {
        PROCEED, PROCEED_ALL, HALT_AT_WRITE, HALT,
        /**
         * Khong phai quyet dinh - la YEU CAU quyet dinh. Agent tra ve day nghia la no da ghi
         * mot ban ghi cho o /dq/decisions/ va Java phai tu doc lap ban ghi do cho toi khi co
         * nguoi tra loi. Xem DqHumanDecision.
         */
        AWAIT_HUMAN;

        /**
         * Phan giai ten hanh dong tu JSON. MOT cho duy nhat, vi truoc day BA cho khac nhau deu
         * viet {@code "HALT".equals(t) ? HALT : PROCEED} - them mot hanh dong moi la phai nho
         * sua ca ba, quen mot cho thi hanh dong do am tham bi ha thanh PROCEED.
         *
         * Khong doc duoc thi ve PROCEED: moi duong that bai cua khau checkpoint deu phai cho
         * pipeline chay tiep, khong duoc bien mot loi phan giai thanh mot lan dung job.
         */
        public static Action parse(String text) {
            if (text == null) return PROCEED;
            try {
                return valueOf(text.trim().toUpperCase());
            } catch (IllegalArgumentException e) {
                return PROCEED;
            }
        }
    }

    /** @param raw full decision document, for logging/report; null when the agent never answered. */
    public record Result(Action action, String reason, JsonNode raw) {
        public boolean isHalt() {
            return action == Action.HALT;
        }

        /** Con duoc ghi khong - HALT_AT_WRITE cho chay tiep nhung cam ghi. */
        public boolean blocksWrite() {
            return action == Action.HALT || action == Action.HALT_AT_WRITE;
        }

        /** Nguoi da tra loi "dung hoi nua" cho ca luot chay nay. */
        public boolean silencesFurtherPrompts() {
            return action == Action.PROCEED_ALL;
        }

        /** Agent xin y kien nguoi - Java phai cho, day chua phai ket qua. */
        public boolean awaitsHuman() {
            return action == Action.AWAIT_HUMAN;
        }
    }

    /**
     * Noi ghi dau vet, do DqGuard gan vao. Static vi DqCheckpoint.call() la static va doi chu ky
     * cua no se pha moi pipeline dang goi. Mot job Spark = mot JVM = mot DqGuard, nen khong co
     * chuyen hai gia tri tranh nhau.
     */
    private static final java.util.concurrent.atomic.AtomicReference<DqCheckpointTrace> TRACE =
            new java.util.concurrent.atomic.AtomicReference<>();

    /** DqGuard goi khi dung len, de duong that bai cua HTTP con cho ghi lai. */
    public static void setTrace(DqCheckpointTrace trace) {
        TRACE.set(trace);
    }

    private static Result proceed(String reason) {
        return new Result(Action.PROCEED, reason, null);
    }

    /**
     * Chay mot checkpoint. KHONG BAO GIO nem exception va khong bao gio tra null.
     *
     * @param checkpoint ten checkpoint (vd "pre_rules", "post_report")
     * @param context    du lieu Java gui cho agent (metrics, schema, dq_key, run_id...)
     */
    public static Result call(String checkpoint, ObjectNode context) {
        return call(checkpoint, context, null);
    }

    /**
     * Goi agent, va neu truyen mot registry khong rong thi MO KENH 2 CHIEU: Java cong bo danh
     * sach tool, agent goi cai no thay can, Java thuc thi roi tra ket qua ve.
     *
     * @param tools tap cong cu checkpoint nay mo ra. null hoac rong = khong mo kenh live.
     */
    public static Result call(String checkpoint, ObjectNode context, DqToolRegistry tools) {
        if (!DqConfig.CHECKPOINTS_ENABLED) {
            LOG.info("=== [checkpoint {}] DQ_AGENT_CHECKPOINTS=false - bo qua", checkpoint);
            return proceed("checkpoints disabled");
        }
        return switch (DqConfig.AGENT_MODE) {
            case PROCESS_ONLY -> callViaProcess(checkpoint, context, tools);
            case HTTP_ONLY -> httpOnly(checkpoint, context, tools);
            case HTTP_THEN_PROCESS -> httpThenProcess(checkpoint, context, tools);
        };
    }

    /**
     * Thu dich vu truoc, hong thi lui ve tien trinh con. DAY LA MAC DINH, va duong lui khong
     * bao gio duoc bo: dich vu chi co mot replica, trang thai phien nam trong RAM, mot lan
     * restart giua phien la giet phien.
     */
    private static Result httpThenProcess(String checkpoint, ObjectNode context,
                                          DqToolRegistry tools) {
        long budgetMs = DqConfig.AGENT_TIMEOUT_SEC * 1000L;
        long startedAt = System.currentTimeMillis();
        DqCheckpointHttp.Outcome out =
                DqCheckpointHttp.call(checkpoint, context, tools, budgetMs, false);
        if (out.ok()) {
            if (tools != null) tools.close();
            return out.result();
        }

        long spent = System.currentTimeMillis() - startedAt;
        long leftMs = budgetMs - spent;
        // Chi lui khi CON DU THOI GIAN de tien trinh con lam duoc viec gi do. Lui voi 3 giay con
        // lai chi ton them mot lan khoi dong Python roi cung bi cat - te hon la PROCEED ngay.
        if (leftMs < DqConfig.HTTP_FALLBACK_MIN_MS) {
            String reason = "dich vu agent hong (" + out.failure() + ") va chi con " + leftMs
                    + "ms - khong lui ve tien trinh con";
            LOG.warn("=== [checkpoint {}] {}", checkpoint, reason);
            recordTrace(context, checkpoint, "http", "http_failed_no_fallback", reason,
                    out.httpStatus());
            if (tools != null) tools.close();
            return proceed(reason);
        }
        LOG.warn("=== [checkpoint {}] dich vu agent hong ({}) - LUI VE tien trinh con, con {}ms",
                checkpoint, out.failure(), leftMs);
        recordTrace(context, checkpoint, "http", "http_failed_fell_back", out.failure(),
                out.httpStatus());
        try {
            return callViaProcess(checkpoint, context, tools);
        } finally {
            if (tools != null) tools.close();
        }
    }

    private static Result httpOnly(String checkpoint, ObjectNode context, DqToolRegistry tools) {
        long budgetMs = DqConfig.AGENT_TIMEOUT_SEC * 1000L;
        DqCheckpointHttp.Outcome out =
                DqCheckpointHttp.call(checkpoint, context, tools, budgetMs, true);
        if (out.ok()) return out.result();
        String reason = "dich vu agent hong va DQ_AGENT_MODE=http_only: " + out.failure();
        LOG.warn("=== [checkpoint {}] {}", checkpoint, reason);
        recordTrace(context, checkpoint, "http", "http_failed", reason, out.httpStatus());
        return proceed(reason);
    }

    /**
     * Ghi mot dong vao /dq/checkpoints de duong that bai khong bien mat.
     *
     * Binh thuong chinh agent ghi dong nay. Khi no khong chay duoc thi KHONG AI ghi, va mot
     * checkpoint hong se trong y het mot checkpoint PROCEED that - dung kieu mau xanh im lang
     * ma ca he nay dang co gang tranh.
     */
    private static void recordTrace(ObjectNode context, String checkpoint, String transport,
                                    String outcome, String reason, int httpStatus) {
        DqCheckpointTrace t = TRACE.get();
        if (t == null) {
            LOG.warn("=== [checkpoint {}] khong co DqCheckpointTrace - duong PROCEED nay se KHONG"
                    + " co ban ghi nao tren /dq/checkpoints", checkpoint);
            return;
        }
        try {
            t.record(checkpoint, Action.PROCEED, reason, transport, outcome, httpStatus);
        } catch (Exception e) {
            LOG.warn("=== [checkpoint {}] khong ghi duoc dau vet: {}", checkpoint, e.toString());
        }
    }

    /** Duong cu: fork mot tien trinh Python. GIU NGUYEN, khong sua logic. */
    private static Result callViaProcess(String checkpoint, ObjectNode context,
                                         DqToolRegistry tools) {

        Path contextFile = null;
        Path decisionFile = null;
        Path logFile = null;
        Process process = null;
        try {
            contextFile = Files.createTempFile("dq-ctx-" + checkpoint + "-", ".json");
            decisionFile = Files.createTempFile("dq-decision-" + checkpoint + "-", ".json");
            logFile = Files.createTempFile("dq-agent-" + checkpoint + "-", ".log");
            Files.writeString(contextFile, MAPPER.writeValueAsString(context), StandardCharsets.UTF_8);

            List<String> cmd = new ArrayList<>(List.of(
                    DqConfig.AGENT_PYTHON, DqConfig.AGENT_ENTRYPOINT,
                    "--checkpoint", checkpoint,
                    "--context-file", contextFile.toString(),
                    "--decision-out", decisionFile.toString()));

            ProcessBuilder pb = new ProcessBuilder(cmd);
            pb.directory(new File(DqConfig.AGENT_DIR));
            // stderr -> FILE (transcript cua agent rat dai, de qua pipe se day buffer va treo).
            // stdout GIU LAI lam pipe: do la kenh giao thuc JSON-lines 2 chieu.
            pb.redirectError(logFile.toFile());
            // De dau vet checkpoint tren HDFS tu noi duoc no di bang duong nao. Dat TUONG MINH
            // du day la mac dinh cua dq_store: duong nay la duong lui, va mot dau vet ghi
            // "process" phai la mot khang dinh, khong phai mot gia tri con sot lai.
            pb.environment().put("DQ_CHECKPOINT_TRANSPORT", "process");
            boolean liveChannel = tools != null && !tools.isEmpty();
            if (liveChannel) {
                pb.environment().put("DQ_LIVE_CHANNEL", "1");
            }
            // Danh tinh lay tu JOB chu khong tu bien moi truong cua pod. Job la noi duy nhat biet
            // chac no dang ghi bang nao (CLI arg 4), con DQ_KEY trong ConfigMap thi chi la mot
            // ban sao co the lech. Lech thi Python ghi rule vao /dq/rules/{key-A}/ trong khi Java
            // doc /dq/rules/{key-B}/ - khong thay rule nao, job van xanh, kiem tra 0 rule.
            // Dat o day la ghi de ban sao do, nen mot pipeline moi khong can khai lai hai bien nay.
            exportEnv(pb, context, "dq_key", "DQ_KEY");
            exportEnv(pb, context, "dataset_name", "DQ_DATASET_NAME");

            LOG.info("=== [checkpoint {}] goi agent: {} (timeout {}s, kenh live: {}, log {})",
                    checkpoint, String.join(" ", cmd), DqConfig.AGENT_TIMEOUT_SEC, liveChannel, logFile);
            long startedAt = System.currentTimeMillis();
            process = pb.start();

            Result fromChannel = runProtocol(process, checkpoint, liveChannel ? tools : null);
            if (fromChannel != null) return fromChannel;

            long tookMs = System.currentTimeMillis() - startedAt;
            int exit = process.exitValue();
            if (exit != 0) {
                LOG.warn("=== [checkpoint {}] agent thoat voi ma {} sau {}ms - tiep tuc (PROCEED). Log: {}",
                        checkpoint, exit, tookMs, logFile);
                return proceed("agent exit code " + exit);
            }

            if (!Files.exists(decisionFile) || Files.size(decisionFile) == 0) {
                LOG.warn("=== [checkpoint {}] agent chay xong nhung KHONG ghi decision - tiep tuc (PROCEED). Log: {}",
                        checkpoint, logFile);
                return proceed("agent produced no decision document");
            }

            JsonNode decision = MAPPER.readTree(Files.readString(decisionFile, StandardCharsets.UTF_8));
            String actionText = decision.path("action").asText("PROCEED");
            String reason = decision.path("reason").asText("");
            Action action = Action.parse(actionText);

            LOG.info("=== [checkpoint {}] agent tra ve {} sau {}ms: {}", checkpoint, action, tookMs, reason);
            return new Result(action, reason, decision);

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            if (process != null) process.destroyForcibly();
            LOG.warn("=== [checkpoint {}] bi interrupt - tiep tuc (PROCEED)", checkpoint);
            return proceed("interrupted");
        } catch (Exception e) {  // checkpoint khong bao gio duoc lam hong job chinh
            if (process != null) process.destroyForcibly();
            LOG.error("=== [checkpoint {}] loi khi goi agent: {} - tiep tuc (PROCEED)",
                    checkpoint, e.toString(), e);
            return proceed("checkpoint error: " + e);
        } finally {
            deleteQuietly(contextFile);
            deleteQuietly(decisionFile);
            // logFile giu lai co chu y: day la dau vet duy nhat de debug agent trong driver pod.
        }
    }

    /**
     * Vong lap giao thuc voi tien trinh agent.
     *
     * @return Result khi agent gui dong "decision"; null khi agent ket thuc ma khong gui gi
     *         (luc do caller quay ve doc file decision nhu duong cu).
     */
    private static Result runProtocol(Process process, String checkpoint, DqToolRegistry tools)
            throws Exception {
        ExecutorService pool = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "dq-checkpoint-" + checkpoint);
            t.setDaemon(true);
            return t;
        });
        try {
            Future<Result> future = pool.submit(() -> {
                try (BufferedReader in = new BufferedReader(
                             new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8));
                     BufferedWriter out = new BufferedWriter(
                             new OutputStreamWriter(process.getOutputStream(), StandardCharsets.UTF_8))) {

                    // Cong bo TRUOC khi doc: agent can biet minh co gi truoc khi quyet dinh.
                    // Chi gui khi that su co tool, de Python khong ngoi cho mot dong khong den.
                    if (tools != null) {
                        ObjectNode hello = MAPPER.createObjectNode();
                        hello.put("type", "hello");
                        hello.put("checkpoint", checkpoint);
                        hello.set("tools", tools.declarations());
                        out.write(MAPPER.writeValueAsString(hello));
                        out.write("\n");
                        out.flush();
                    }

                    String line;
                    while ((line = in.readLine()) != null) {
                        line = line.trim();
                        if (line.isEmpty() || line.charAt(0) != '{') {
                            continue;   // dong la khong phai giao thuc - bo qua cho chac
                        }
                        JsonNode msg;
                        try {
                            msg = MAPPER.readTree(line);
                        } catch (Exception parseErr) {
                            continue;
                        }
                        String type = msg.path("type").asText("");

                        if ("tool_request".equals(type)) {
                            ObjectNode resp = tools == null
                                    ? (ObjectNode) MAPPER.createObjectNode().put("type", "tool_response")
                                        .put("ok", false).put("error", "Job khong mo kenh live cho checkpoint nay.")
                                    : tools.handle(msg.path("tool").asText(""), msg.path("args"));
                            out.write(MAPPER.writeValueAsString(resp));
                            out.write("\n");
                            out.flush();

                        } else if ("decision".equals(type)) {
                            String actionText = msg.path("action").asText("PROCEED");
                            Action action = Action.parse(actionText);
                            String reason = msg.path("reason").asText("");
                            LOG.info("=== [checkpoint {}] agent quyet dinh {} qua kenh live: {}",
                                    checkpoint, action, reason);
                            return new Result(action, reason, msg);
                        }
                    }
                }
                return null;   // agent dong stdout ma khong gui decision
            });

            Result r = future.get(DqConfig.AGENT_TIMEOUT_SEC, TimeUnit.SECONDS);
            process.waitFor(10, TimeUnit.SECONDS);   // de agent ghi not HDFS/log roi thoat
            return r;

        } catch (java.util.concurrent.TimeoutException te) {
            process.destroyForcibly();
            LOG.warn("=== [checkpoint {}] agent QUA HAN {}s - tiep tuc pipeline (PROCEED)",
                    checkpoint, DqConfig.AGENT_TIMEOUT_SEC);
            return proceed("agent timeout after " + DqConfig.AGENT_TIMEOUT_SEC + "s");
        } finally {
            pool.shutdownNow();
        }
    }

    /** Dua mot truong cua context sang bien moi truong cua tien trinh agent. */
    private static void exportEnv(ProcessBuilder pb, ObjectNode context, String field, String envVar) {
        String v = context.path(field).asText(null);
        if (v != null && !v.isBlank()) pb.environment().put(envVar, v);
    }

    private static void deleteQuietly(Path p) {
        if (p == null) return;
        try {
            Files.deleteIfExists(p);
        } catch (Exception ignored) {
            // temp file, khong dang de lam on
        }
    }
}
