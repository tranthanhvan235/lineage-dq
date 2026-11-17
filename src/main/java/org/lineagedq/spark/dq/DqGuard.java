package org.lineagedq.spark.dq;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

/**
 * Cua duy nhat mot pipeline can biet de gan framework DQ vao. Ba loi goi:
 *
 * <pre>
 * try (DqGuard dq = DqGuard.builder()
 *         .spark(spark).appId(appId)
 *         .dqKey(cfg.getOutTable())
 *         .datasetName("/warehouse/" + cfg.getOutDb() + ".db/" + cfg.getOutTable())
 *         .warehouse(cfg.getOutDb())
 *         .hadoop(hadoopConf, hdfsBaseUri)
 *         .lineageSource(appId -&gt; enricher.fetchCompleteEvent(appId, 8))
 *         .build()) {
 *
 *     result = dq.beforeWrite(result);          // checkpoint + doc rule
 *     MyWriter.write(spark, result, ...);       // action DUY NHAT
 *     dq.afterWrite();                          // report + checkpoint + moi bang Iceberg
 * }
 * </pre>
 *
 * Ba bat bien nam ben trong, khong phu thuoc vao viec nguoi tich hop co nho hay khong:
 *   1. Dataset tra ve tu beforeWrite() la Dataset PHAI dung de ghi.
 *   2. Moi buoc nuot loi - DQ khong bao gio lam hong pipeline.
 *   3. afterWrite() chay TRUOC spark.stop() vi bang Iceberg can SparkSession.
 */
public final class DqGuard implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(DqGuard.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    /**
     * Nguon lineage cua mot lan chay. Tach thanh interface de goi dq khong phu thuoc goi lineage.
     * Phai loc theo outputDataset: chinh cac lenh ghi bang DQ cung sinh event lineage.
     */
    @FunctionalInterface
    public interface LineageSource {
        JsonNode fetch(String appId, String outputDataset) throws Exception;
    }

    private final SparkSession spark;
    private final String appId;
    private final String dqKey;
    private final String datasetName;
    private final String warehouseDb;
    private final Configuration hadoopConf;
    private final String hdfsBaseUri;
    private final String stageView;
    private final List<DqTool> extraTools;
    private final LineageSource lineageSource;
    private final LineageSource lineageFallback;
    private final IcebergSink sink;

    private Dataset<Row> cached;
    private DqRuleSet ruleSet = DqRuleSet.empty();

    // ── Trang thai cua duong gate() theo stage ──────────────────────────────
    private boolean rulesLoaded;
    /** Nguoi da tra loi PROCEED_ALL - khong hoi lai trong luot chay nay nua. */
    private boolean humanPromptsSilenced;
    /** Mot checkpoint da quyet HALT_AT_WRITE hoac HALT - lenh ghi bi cam. */
    private String writeBlockedBy;
    private String writeBlockedReason;
    /**
     * Lenh cam ghi den tu CONG HOI NGUOI, khong phai tu agent tu quyet.
     *
     * DQ_GATE_ENFORCE=false la che do quan sat: agent bao HALT thi chi canh bao roi chay tiep.
     * Nhung mot nguoi da chu dong bam HALT_AT_WRITE ma he thong van ghi thi la sai han - quyet
     * dinh cua nguoi khong duoc mot bien moi truong vo hieu hoa. Cong hoi nguoi cung chi mo ra
     * khi dev tu dat on_fail=ask_human cho checkpoint do, tuc da la mot lan chon co y.
     */
    private boolean writeBlockedByHumanGate;
    private int gateOrder;
    private final List<ObjectNode> stageReports = new ArrayList<>();
    private final ArrayNode stageManifest = MAPPER.createArrayNode();
    /** Cache cua cac gate gan nhat; bo dan de khong giu het moi tang trong bo nho. */
    private final Deque<Dataset<Row>> gateCaches = new ArrayDeque<>();

    private DqGuard(Builder b) {
        DqLogLevel.ensureInfo();   // truoc moi thu khac, khong thi chinh dong log dau bi nuot
        this.spark = b.spark;
        this.appId = b.appId;
        this.dqKey = b.dqKey;
        this.datasetName = b.datasetName;
        this.warehouseDb = b.warehouseDb;
        this.hadoopConf = b.hadoopConf;
        this.hdfsBaseUri = b.hdfsBaseUri;
        this.stageView = b.stageView;
        this.extraTools = b.extraTools;
        this.lineageSource = b.lineageSource;
        this.lineageFallback = b.lineageFallback;
        // Duong HTTP co the that bai TRUOC khi agent kip chay - luc do khong ai ghi dong nao vao
        // /dq/checkpoints, va mot checkpoint hong se trong y het mot checkpoint PROCEED that.
        // Dua cho DqCheckpoint cho ghi ngay tu day.
        DqCheckpoint.setTrace(new DqCheckpointTrace(b.hadoopConf, b.hdfsBaseUri, b.dqKey, b.appId));
        this.sink = (DqConfig.WAREHOUSE_ENABLED && b.warehouseDb != null)
                ? new IcebergSink(b.spark, DqConfig.WAREHOUSE_CATALOG, b.warehouseDb) : null;
    }

    public static Builder builder() {
        return new Builder();
    }

    private ObjectNode report;

    /**
     * Mot checkpoint do dev tu dat giua pipeline: sau khi doc nguon, sau mot JOIN, sau mot phep
     * bien doi. Moi lan goi la mot diem do RIENG BIET, co diem chat luong rieng.
     *
     * <pre>
     * raw    = dq.gate("src_calls", raw);      // sau khi doc nguon
     * joined = dq.gate("jn_map1", joined);     // sau JOIN
     * result = dq.gate("final_select", result);
     * writer.write(result);
     * dq.afterWrite();
     * </pre>
     *
     * Gate EP MOT ACTION (aggregate rieng, xem DqMetricQuery) de co so ngay tai cho, roi hoi
     * agent PROCEED/HALT truoc khi pipeline chay tiep. Doi lai moi gate ton them mot luot
     * quet, nen cache lai cho buoc sau dung.
     *
     * @throws DqHaltException khi agent quyet dinh HALT va DQ_GATE_ENFORCE=true
     */
    /**
     * Dat mot checkpoint chat luong tai dung cho nay trong code.
     *
     * DO NGAY va CHAN: ham chi tra ve khi da co ket luan chat luong cho checkpoint nay. Dong
     * code ke tiep chi chay sau do; neu agent quyet HALT thi ham nem {@link DqHaltException}
     * va dong do khong bao gio chay.
     *
     * Tra ve void co chu y. Ban cu tra ve Dataset va bat caller gan lai bien - quen dau '='
     * la mot lo hong am tham: checkpoint van chay nhung khong con tac dung nao. Da go han
     * ban do 21/09/2026 sau khi ca hai pipeline chuyen sang day.
     *
     * KHONG doi hoi dev khai them gi ve quan he giua cac checkpoint. Thu tu lay tu thu tu goi,
     * schema lay tu df, con cha thi suy bang do trung cot ben phia sinh rule - da kiem tren
     * pipeline nay: tf_decode duoc suy dung ve src_calls chu khong phai src_infra dung lien
     * truoc no. Mot tham so khong bat buoc la mot tham so co the khai sai.
     */
    public void check(String stage, Dataset<Row> df) {
        if (!DqConfig.GATES_ENABLED) return;
        int order = ++gateOrder;
        long t0 = System.currentTimeMillis();
        try {
            ensureRulesLoaded(df);
            List<DqRule> rules = ruleSet.forStage(stage);
            recordStage(stage, order, df, rules.size(), null);
            snapshot(stage, df);

            // Tra POLICY truoc khi lam bat ky viec Spark nao - day la cho DUY NHAT cat duoc
            // chi phi. Policy ve sau decision thi da do xong roi, khong con tac dung.
            String measure = policyOf(stage, "measure", rules.isEmpty() ? "skip" : "full");
            if ("skip".equals(measure) || rules.isEmpty()) {
                LOG.info("=== [gate {}] measure={} rule={} -> bo qua, 0 action.",
                        stage, measure, rules.size());
                return;
            }

            // Cache chinh df GOC (khong phai ban da bien doi): gate ep action nen khong cache
            // thi moi gate quet lai DAG tu nguon - rieng tf_decode la decode protobuf + explode
            // lai tu dau. Ref-count o retainCache: KHONG BAO GIO unpersist mot dataset ma buoc
            // sau con dung, do la loi cua FIFO depth-2 truoc day (no duoi src_calls dung ngay
            // truoc tf_decode).
            retainCache(df);

            long tm = System.currentTimeMillis();
            DqMetricQuery.Measured m = DqMetricQuery.measure(df, rules);   // 1 action
            long measureMs = System.currentTimeMillis() - tm;

            ObjectNode stageReport = DqReportWriter.buildStageReport(
                    stage, order, datasetName, appId, ruleSet,
                    m.attachedRules(), m.metrics(), rules.size(), m.droppedRules());
            long rows = stageReport.path("total_rows").asLong();
            stageReport.put("measure_ms", measureMs);
            stageReport.put("measured_on", "recompute");
            recordStage(stage, order, df, rules.size(), rows);

            LOG.info("=== [gate {}] rows={} rule={}/{} status={} score={}",
                    stage, rows, m.attachedRules().size(), rules.size(),
                    stageReport.path("status").asText(), stageReport.path("quality_score").asText());

            long ta = System.currentTimeMillis();
            askAgent(stage, order, stageReport, df);
            stageReport.put("agent_wait_ms", System.currentTimeMillis() - ta);
            stageReport.put("gate_total_ms", System.currentTimeMillis() - t0);

            stageReports.add(stageReport);
            publishPartialReport();   // diem cua gate nay khong bay hoi neu gate sau HALT

            // Tra df NGUYEN VEN: khong CollectMetrics, khong gi ca. Caller khong bat buoc
            // phai gan lai bien - do la chu y, vi quen gan lai tung la lo hong am tham.
            return;

        } catch (DqHaltException e) {
            throw e;                    // co y dung job - khong duoc nuot
        } catch (Exception e) {
            LOG.error("=== [gate {}] that bai (bo qua, pipeline chay tiep): {}", stage, e.toString(), e);
            return;
        }
    }

    /** Doc policy cua mot stage tu khoi `policy` nhung trong active.json, khong co thi lay mac dinh. */
    private String policyOf(String stage, String key, String fallback) {
        JsonNode v = ruleSet.policyFor(stage).path(key);
        if (!v.isMissingNode() && !v.isNull() && !v.asText().isBlank()) return v.asText();
        if ("ask_agent".equals(key)) {
            return DqConfig.GATE_ASK_ALWAYS.contains(stage) ? "always" : DqConfig.GATE_ASK_DEFAULT;
        }
        return fallback;
    }

    /**
     * Chup checkpoint ra parquet cho thuc nghiem (DQ_SNAPSHOT_DIR). Chup CA khi stage khong co
     * rule nao: testbed phai doc lap voi bo rule, vi moi phuong phap trong thuc nghiem sinh mot
     * bo rule khac nhau tren cung du lieu. Cache truoc de lan do ke tiep khong tinh lai tu dau.
     */
    private void snapshot(String stage, Dataset<Row> df) {
        if (DqConfig.SNAPSHOT_DIR.isEmpty()) return;
        String path = DqConfig.SNAPSHOT_DIR + "/" + dqKey + "/" + appId + "/" + stage;
        try {
            retainCache(df);
            Dataset<Row> out = DqConfig.SNAPSHOT_FRACTION < 1.0
                    ? df.sample(false, DqConfig.SNAPSHOT_FRACTION, 42L) : df;
            out.write().mode("overwrite").parquet(path);
            LOG.info("=== [gate {}] snapshot -> {} (fraction={})", stage, path, DqConfig.SNAPSHOT_FRACTION);
        } catch (Exception e) {
            // Vd cot trung ten o stage nay: parquet tu choi ghi. Mat mot snapshot, khong mat job.
            LOG.warn("=== [gate {}] khong chup duoc snapshot {}: {}", stage, path, e.toString());
        }
    }

    /** Giu cache va dem so nguoi dung; chi unpersist o release(). */
    private void retainCache(Dataset<Row> df) {
        if (DqConfig.GATE_CACHE_DEPTH <= 0) return;
        for (Dataset<Row> d : gateCaches) {
            if (d == df) return;      // da cache roi, khong cache chong
        }
        df.cache();
        gateCaches.addLast(df);
    }

    /** Hoi agent tai cho. HALT chi thuc su dung job khi DQ_GATE_ENFORCE=true. */
    private void askAgent(String stage, int order, ObjectNode stageReport, Dataset<Row> df) {
        String ask = policyOf(stage, "ask_agent", DqConfig.GATE_ASK_DEFAULT);
        boolean clean = "PASS".equals(stageReport.path("status").asText())
                && stageReport.path("rules_declared").asInt() == stageReport.path("rules_attached").asInt();
        if ("never".equals(ask) || ("on_violation".equals(ask) && clean)) {
            stageReport.put("agent_action", "AUTO_PROCEED");
            stageReport.put("agent_reason", "policy ask_agent=" + ask + " va stage sach - khong goi agent");
            return;
        }

        ObjectNode ctx = baseContext();
        ctx.put("stage", stage);
        ctx.put("stage_order", order);
        ctx.set("stage_report", stageReport);
        ctx.put("live_stage_view", stageView);
        // Checkpoint nay co duoc phep chan cho hoi nguoi khong - do dev dat trong policy, khong
        // do agent tu quyet. Hoi nguoi o moi chang la cach nhanh nhat de khong ai con doc nua.
        String onFail = policyOf(stage, "on_fail", DqConfig.GATE_ON_FAIL_DEFAULT);
        ctx.put("on_fail_policy", humanPromptsSilenced ? "warn" : onFail);
        ctx.put("human_prompts_silenced", humanPromptsSilenced);
        if (writeBlockedBy != null) {
            ctx.put("write_already_blocked_by", writeBlockedBy);
        }

        // Mo tool doc du lieu song cua CHINH stage nay. Truoc day cho nay truyen null nen
        // query_live_stage khong bao gio kha dung o gate - do la ly do that khien no chua tung
        // duoc goi, khong phai vi agent khong muon dung.
        DqToolRegistry tools = new DqToolRegistry();
        try {
            df.createOrReplaceTempView(stageView);
            tools.add(new DqLiveQuery(spark, stageView));
            for (DqTool t : extraTools) tools.add(t);
        } catch (Exception e) {
            LOG.warn("=== [gate {}] khong mo duoc view du lieu song: {}", stage, e.toString());
        }

        DqCheckpoint.Result d = DqCheckpoint.call("gate_" + stage, ctx, tools);
        // Quyet dinh di qua cong hoi nguoi thi RANG BUOC, ke ca khi het gio ve mac dinh: cong
        // do chi mo khi dev dat on_fail=ask_human, nen di qua no da la mot lua chon co y.
        boolean fromHumanGate = false;

        // Agent xin y kien nguoi: phien agent da ket thuc sach, gio Java tu cho. Vong cho KHONG
        // duoc nam trong tool cua agent - xem DqHumanDecision de biet vi sao (chot
        // HTTP_MAX_NO_PROGRESS se giet phien sau ~66 phut va tra PROCEED).
        if (d.awaitsHuman()) {
            stageReport.put("agent_action", d.action().name());
            stageReport.put("agent_reason", d.reason());
            d = DqHumanDecision.await(hadoopConf, hdfsBaseUri, dqKey, appId, stage);
            fromHumanGate = true;
            stageReport.put("human_action", d.action().name());
            stageReport.put("human_reason", d.reason());
        }
        stageReport.put("decision_binding", fromHumanGate);

        stageReport.put("agent_action", d.action().name());
        stageReport.put("agent_reason", d.reason());

        if (d.silencesFurtherPrompts()) {
            humanPromptsSilenced = true;
            LOG.warn("=== [gate {}] nguoi tra loi PROCEED_ALL - khong hoi lai trong luot nay.", stage);
        }

        // HALT_AT_WRITE: chay tiep de chan doan het cac chang, nhung cam ghi. Ghi lai chang
        // DAU TIEN chan - cac chang sau thuong chi la he qua lan xuong cua cung mot nguyen nhan.
        if (d.blocksWrite() && writeBlockedBy == null) {
            writeBlockedBy = stage;
            writeBlockedReason = d.reason();
            writeBlockedByHumanGate = fromHumanGate;
        }
        if (d.action() == DqCheckpoint.Action.HALT_AT_WRITE) {
            LOG.warn("=== [gate {}] HALT_AT_WRITE: chay tiep de chan doan, nhung SE KHONG GHI. {}",
                    stage, d.reason());
            return;
        }
        if (!d.isHalt()) return;

        if (DqConfig.GATE_ENFORCE || fromHumanGate) {
            LOG.error("=== [gate {}] HALT: {} - DUNG JOB ({})", stage, d.reason(),
                    fromHumanGate ? "nguoi quyet dinh" : "DQ_GATE_ENFORCE=true");
            throw new DqHaltException(stage, d.reason());
        }
        LOG.warn("=== [gate {}] HALT: {} - nhung DQ_GATE_ENFORCE=false nen chay tiep.",
                stage, d.reason());
    }

    /**
     * Cong ghi: goi NGAY TRUOC lenh ghi, nem neu mot checkpoint nao do da cam ghi.
     *
     * Day la cho duy nhat trong ca framework thuc su bao ve muc tieu "khong ghi du lieu rac
     * xuong": lenh ghi la hanh dong duy nhat khong hoan tac duoc. Cac checkpoint truoc do van
     * chan duoc som (HALT) de du lieu hong khong chay tiep sang buoc sau va lam hong tiep;
     * cong nay bat not truong hop nguoi chon HALT_AT_WRITE - cho chay het de chan doan day du
     * roi moi chan dung cho can chan.
     *
     * @throws DqHaltException khi mot checkpoint da quyet HALT hoac HALT_AT_WRITE
     */
    public void beforeWrite() {
        if (writeBlockedBy == null) return;
        String reason = "checkpoint '" + writeBlockedBy + "' da cam ghi: " + writeBlockedReason;
        if (!DqConfig.GATE_ENFORCE && !writeBlockedByHumanGate) {
            LOG.warn("=== CONG GHI: {} - nhung DQ_GATE_ENFORCE=false nen VAN GHI.", reason);
            return;
        }
        LOG.error("=== CONG GHI: {} - KHONG GHI.", reason);
        throw new DqHaltException(writeBlockedBy, reason);
    }

    /**
     * Lan gate dau tien chay checkpoint "pre_rules" roi moi doc rule: agent phai co co hoi sinh
     * rule TRUOC gate dau, khong thi ca lan chay khong do duoc gi.
     *
     * Agent sinh rule tu manifest cua LAN CHAY TRUOC (/dq/stages/{key}/latest.json) - cung mo
     * hinh voi lineage, vi schema tung stage chi biet duoc khi da chay qua stage do.
     */
    private void ensureRulesLoaded(Dataset<Row> firstDf) {
        if (rulesLoaded) return;
        rulesLoaded = true;
        try {
            firstDf.createOrReplaceTempView(stageView);
            ObjectNode ctx = baseContext();
            ctx.put("stage", "pre_rules");
            ctx.put("live_stage_view", stageView);
            // Manifest do chinh lan chay TRUOC ghi ra (afterWrite). O LAN CHAY DAU TIEN no
            // chua ton tai, va framework khong co cach nao biet truoc danh sach checkpoint -
            // chung chi lo ra khi tung dq.check() chay. Phai NOI RA dieu do, khong thi agent
            // sinh rule mu: da xay ra that tren cell_capacity_daily - Gen Rules thu 3 lan,
            // Verify bac ca 3 vi moi rule deu 'misplaced', va transcript ba lan cong lai tran
            // cua so ngu canh (281K > 262K token). Ba lan hong vi mot dieu le ra noi mot cau.
            JsonNode manifest = DqJson.read(hadoopConf, hdfsBaseUri,
                    hdfsBaseUri + "/dq/stages/" + dqKey + "/latest.json");
            boolean firstRun = manifest == null || !manifest.path("stages").elements().hasNext();
            ctx.put("stage_manifest_available", !firstRun);
            ctx.put("note", firstRun
                    ? "LAN CHAY DAU TIEN cua pipeline nay: /dq/stages/" + dqKey + "/latest.json"
                      + " CHUA TON TAI, nen khong ai biet pipeline co nhung checkpoint nao."
                      + " DUNG sinh rule o lan nay - moi rule deu se bi dat sai stage va bi"
                      + " tham dinh bac. Hay ket thuc bang skip_generation voi ly do 'chua co"
                      + " manifest checkpoint'. Lan chay nay se ghi manifest ra; sinh rule o"
                      + " lan chay SAU."
                    : "Pipeline dung checkpoint theo stage. Doc /dq/stages/" + dqKey
                      + "/latest.json de biet danh sach checkpoint va schema tung diem.");
            DqToolRegistry tools = new DqToolRegistry().add(new DqLiveQuery(spark, stageView));
            for (DqTool t : extraTools) tools.add(t);
            DqCheckpoint.Result pre = DqCheckpoint.call("pre_rules", ctx, tools);
            if (pre.isHalt()) {
                LOG.warn("=== [pre_rules] agent khuyen nghi HALT: {}", pre.reason());
            }
        } catch (Exception e) {
            LOG.error("=== [pre_rules] that bai (bo qua): {}", e.toString());
        }
        ruleSet = DqRuleLoader.loadActive(hadoopConf, hdfsBaseUri, dqKey);
    }

    /** Ghi/ cap nhat mot muc trong manifest stage. rows=null khi chua ep action. */
    private void recordStage(String stage, int order, Dataset<Row> df, int ruleCount, Long rows) {
        for (JsonNode n : stageManifest) {
            if (n.path("stage").asText().equals(stage)) {
                if (rows != null) ((ObjectNode) n).put("row_count", rows);
                return;
            }
        }
        ObjectNode m = stageManifest.addObject();
        m.put("stage", stage);
        m.put("order", order);
        m.put("rule_count", ruleCount);
        // Ten cot THOI la chua du: agent viet rule mu ve kieu. Vi du that tren pipeline nay -
        // side/call_id kieu integer nhung rule dang dung TRIM(side) = '' nen khong bao gio kich
        // hoat. Ghi luon ca ten cot TRUNG NHAU: jn_map2 co imsi/msisdn lap 2 lan (86+3=89) nen
        // rule tham chieu `imsi` o do se AMBIGUOUS_REFERENCE.
        ArrayNode cols = m.putArray("columns");
        ArrayNode typed = m.putArray("schema");
        java.util.Map<String, Integer> seen = new java.util.LinkedHashMap<>();
        for (org.apache.spark.sql.types.StructField f : df.schema().fields()) {
            cols.add(f.name());
            ObjectNode c = typed.addObject();
            c.put("name", f.name());
            c.put("type", f.dataType().simpleString());
            seen.merge(f.name(), 1, Integer::sum);
        }
        ArrayNode dup = m.putArray("duplicate_names");
        seen.forEach((n, k) -> { if (k > 1) dup.add(n); });
    }


    /**
     * Copy NDJSON su kien OpenLineage tho tu dia driver len HDFS.
     *
     * Bat buoc phai lam trong job: file nam o /tmp cua driver pod, ma pod o trang thai Completed
     * thi khong exec vao duoc nua - khong copy luc con song thi mat han. Day la nguon duy nhat
     * cho "raw lineage ma OpenLineage ban ra" theo dung nghia tho.
     */
    private void publishRawEvents() {
        if (hadoopConf == null || hdfsBaseUri == null) return;
        java.nio.file.Path f = java.nio.file.Paths.get(DqConfig.LINEAGE_RAW_FILE);
        try {
            if (!java.nio.file.Files.exists(f)) {
                LOG.warn("=== DqGuard: khong thay {} - khong co raw event de luu.", f);
                return;
            }
            byte[] bytes = java.nio.file.Files.readAllBytes(f);
            String dst = hdfsBaseUri + "/dq/raw-events/" + dqKey + "/" + appId + ".ndjson";
            if (DqJson.write(hadoopConf, hdfsBaseUri, dst, bytes)) {
                LOG.info("=== DqGuard: da luu raw OpenLineage ({} byte) -> {}", bytes.length, dst);
            }
        } catch (Exception e) {
            LOG.error("=== DqGuard: luu raw event that bai (bo qua): {}", e.toString());
        }
    }

    /**
     * Publish ban report da co NGAY sau moi gate, danh dau partial.
     *
     * Truoc day report chi publish trong afterWrite(), nen mot gate HALT lam BAY SACH diem cua
     * cac gate da qua - dung cai ma nguoi dung can nhat de biet vi sao bi chan.
     */
    private void publishPartialReport() {
        if (stageReports.isEmpty() || hadoopConf == null || hdfsBaseUri == null) return;
        try {
            ObjectNode doc = DqReportWriter.buildPipelineReport(datasetName, appId, ruleSet, stageReports);
            doc.put("partial", true);
            DqReportWriter.publish(hadoopConf, hdfsBaseUri, dqKey, doc, appId);
        } catch (Exception e) {
            LOG.debug("publish partial report that bai (bo qua): {}", e.toString());
        }
    }

    /** Tai lieu manifest, dung chung cho ca ghi HDFS lan ghi bang Iceberg. */
    private ObjectNode buildManifestDoc() {
        ObjectNode doc = MAPPER.createObjectNode();
        doc.put("dq_key", dqKey);
        doc.put("dataset_name", datasetName);
        doc.put("run_id", appId);
        doc.put("captured_at", java.time.Instant.now().toString());
        doc.set("stages", stageManifest);
        return doc;
    }

    /**
     * Danh sach checkpoint + schema tung diem, de Gen Rules agent biet co the dat rule o dau va
     * cot nao co mat o do. Doc boi dq-agents (dq_store.stages_latest_path).
     */
    private void publishStageManifest() {
        if (stageManifest.isEmpty() || hadoopConf == null || hdfsBaseUri == null) return;
        try {
            byte[] bytes = DqJson.bytes(buildManifestDoc().toPrettyString());
            String base = hdfsBaseUri + "/dq/stages/" + dqKey;
            DqJson.write(hadoopConf, hdfsBaseUri, base + "/latest.json", bytes);
            DqJson.write(hadoopConf, hdfsBaseUri, base + "/history/" + appId + ".json", bytes);
            LOG.info("=== DqGuard: da ghi manifest {} checkpoint -> {}/latest.json",
                    stageManifest.size(), base);
        } catch (Exception e) {
            LOG.error("=== DqGuard: ghi manifest stage that bai (bo qua): {}", e.toString());
        }
    }

    /** Report + checkpoint "post_report" + cac bang Iceberg. PHAI goi TRUOC spark.stop(). */
    public void afterWrite() {
        // THU TU o day khong tuy tien:
        //   1. awaitListenerBus()  - doi listener xu ly xong su kien da post. Tha cache truoc
        //      buoc nay tung lam mat column lineage (da vá; do lai hom nay van du 3 inputs +
        //      79 field ke ca khi co InMemoryRelation trong plan).
        //   2. publishRawEvents()  - file NDJSON nam tren dia driver, pod Completed la mat.
        //   3. release()           - tha cache TRUOC checkpoint post_report, vi checkpoint do
        //      co the giu executor nam khong toi DQ_AGENT_TIMEOUT_SEC. Luc nay lenh ghi da
        //      xong nen khong ai con can cac dataframe nguon nua.
        awaitListenerBus();
        publishStageManifest();
        publishRawEvents();
        release();

        if (!stageReports.isEmpty()) {
            afterWriteStaged();
            return;
        }

        // Khong stage nao chay -> KHONG im lang return. Xem DqReportWriter
        // .buildNoMeasurementReport de biet vi sao: bo qua buoc nay se de latest.json giu
        // report cua lan chay TRUOC, va mot lan chay do 0 rule trong y het mot lan thanh cong.
        List<String> declaredStages = new ArrayList<>();
        for (JsonNode n : stageManifest) declaredStages.add(n.path("stage").asText());
        String reason = ruleSet.rules().isEmpty()
                ? "No rules in active.json (agent could not generate one, or the rule set was "
                  + "rejected in verification) - 0/" + declaredStages.size()
                  + " checkpoints measured."
                : ruleSet.rules().size() + " rules loaded but none attached to any checkpoint"
                  + " - check the 'stage' field of each rule.";
        LOG.error("=== DqGuard.afterWrite: NOTHING MEASURED. {}", reason);
        try {
            report = DqReportWriter.buildNoMeasurementReport(
                    datasetName, appId, ruleSet, declaredStages, reason);
            DqReportWriter.publish(hadoopConf, hdfsBaseUri, dqKey, report, appId);
            LOG.info("=== DqGuard: published no-measurement report (worst_status=NO_RULES_APPLIED)");
        } catch (Exception e) {
            LOG.error("=== DqGuard: failed to publish no-measurement report: {}", e.toString());
            report = null;
        }
        storeLineage();

        if (report == null) return;
        try {
            ObjectNode ctx = baseContext();
            ctx.set("report", report);
            // Khong mo kenh live o day: du lieu da ghi xong, agent phan tich report chu khong
            // soi du lieu nua.
            DqCheckpoint.Result post = DqCheckpoint.call("post_report", ctx);
            LOG.info("=== [post_report] khuyen nghi cua agent: {} - {}", post.action(), post.reason());
        } catch (Exception e) {
            LOG.error("=== DqGuard: checkpoint post_report that bai (bo qua): {}", e.toString());
        }

        // Chay SAU checkpoint cuoi de bat duoc CA hai checkpoint cua lan chay nay.
        if (sink != null) {
            DqWarehouseWriter.appendAgentRuns(sink, hadoopConf, hdfsBaseUri, dqKey, appId);
            DqWarehouseWriter.appendAnalysis(sink, hadoopConf, hdfsBaseUri, dqKey, appId);
        }
    }

    /**
     * Duong afterWrite cua pipeline dung gate(): moi stage da co report rieng tu luc do,
     * o day chi gop lai thanh mot tai lieu roi publish.
     *
     * KHONG tinh diem tong: xem DqReportWriter.buildStageReport. "worst_status" chi de doc nhanh.
     */
    private void afterWriteStaged() {
        try {
            report = DqReportWriter.buildPipelineReport(datasetName, appId, ruleSet, stageReports);
            LOG.info("=== DQ report theo stage: {} checkpoint, worst_status={}",
                    report.path("stage_count").asInt(), report.path("worst_status").asText());
            for (ObjectNode sr : stageReports) {
                LOG.info("===   [{}] {} rows={} quality_score={}", sr.path("stage").asText(),
                        sr.path("status").asText(), sr.path("total_rows").asLong(),
                        sr.path("quality_score").asText());
            }
            DqReportWriter.publish(hadoopConf, hdfsBaseUri, dqKey, report, appId);
        } catch (Exception e) {
            LOG.error("=== DqGuard: publish report theo stage that bai (bo qua): {}", e.toString());
            report = null;
        }

        if (sink != null) {
            // Moi stage mot lan append: bang dq_report_history co cot `stage` de phan biet.
            for (ObjectNode sr : stageReports) {
                ObjectNode row = sr.deepCopy();
                row.put("run_id", appId);
                row.put("dataset_name", datasetName);
                DqWarehouseWriter.appendReport(sink, dqKey, row);
            }
            DqWarehouseWriter.appendRules(sink, dqKey, ruleSet, ruleSet.rules());
            // Manifest va verdict cung phai vao kho: khong co chung thi dashboard va cac cau
            // SQL theo checkpoint khong co gi de dung tren.
            DqWarehouseWriter.appendStageManifest(sink, dqKey, buildManifestDoc());
            DqWarehouseWriter.appendRuleVerification(sink, dqKey,
                    DqRuleLoader.readVerdict(hadoopConf, hdfsBaseUri, dqKey, ruleSet));
        }
        storeLineage();

        if (report != null) {
            try {
                ObjectNode ctx = baseContext();
                ctx.set("report", report);
                DqCheckpoint.Result post = DqCheckpoint.call("post_report", ctx);
                LOG.info("=== [post_report] khuyen nghi cua agent: {} - {}", post.action(), post.reason());
            } catch (Exception e) {
                LOG.error("=== DqGuard: checkpoint post_report that bai (bo qua): {}", e.toString());
            }
        }

        if (sink != null) {
            DqWarehouseWriter.appendAgentRuns(sink, hadoopConf, hdfsBaseUri, dqKey, appId);
            DqWarehouseWriter.appendAnalysis(sink, hadoopConf, hdfsBaseUri, dqKey, appId);
        }
    }

    private void storeLineage() {
        if (!DqConfig.LINEAGE_STORE_ENABLED) return;
        try {
            JsonNode event = lineageSource == null ? null : lineageSource.fetch(appId, datasetName);

            // Marquez chi la duong lui, mac dinh tat: no la lop hien thi, khong phai nguon du lieu.
            if (event == null && lineageFallback != null && DqConfig.LINEAGE_MARQUEZ_FALLBACK) {
                LOG.warn("=== DqGuard: khong co lineage goc - quay ve doc Marquez (duong lui).");
                event = lineageFallback.fetch(appId, datasetName);
            }
            if (event == null) return;

            // HDFS truoc: do la nguon agent Python doc, va no khong can Spark.
            publishLineage(event);
            if (sink != null) DqLineageWriter.append(sink, dqKey, appId, event);
        } catch (Exception e) {
            LOG.error("=== DqGuard: luu lineage that bai (bo qua): {}", e.toString());
        }
    }

    /** Ghi lineage ra /dq/lineage/{key}/ - giu nguyen facet goc de lineage_client.py doc duoc. */
    private void publishLineage(JsonNode event) {
        if (hadoopConf == null || hdfsBaseUri == null) return;
        JsonNode facet = null;
        for (JsonNode out : event.path("outputs")) {
            String name = out.path("name").asText("");
            if (!name.equals(datasetName) && !name.endsWith(datasetName)) continue;
            JsonNode cl = out.path("facets").path("columnLineage");
            if (!cl.isMissingNode() && !cl.isNull()) {
                facet = cl;
                break;
            }
        }
        ObjectNode doc = MAPPER.createObjectNode();
        doc.put("dq_key", dqKey);
        doc.put("run_id", appId);
        doc.put("dataset_name", datasetName);
        doc.put("captured_at", java.time.Instant.now().toString());
        doc.put("source", "openlineage_raw_event");
        doc.set("column_lineage", facet == null ? MAPPER.createObjectNode() : facet.deepCopy());
        doc.set("raw_event", event.deepCopy());

        byte[] bytes = DqJson.bytes(doc.toPrettyString());
        String base = hdfsBaseUri + "/dq/lineage/" + dqKey;
        DqJson.write(hadoopConf, hdfsBaseUri, base + "/latest.json", bytes);
        DqJson.write(hadoopConf, hdfsBaseUri, base + "/history/" + appId + ".json", bytes);
        LOG.info("=== DqGuard: da ghi lineage -> {}/latest.json ({} cot)",
                base, facet == null ? 0 : facet.path("fields").size());
    }

    private ObjectNode baseContext() {
        ObjectNode ctx = MAPPER.createObjectNode();
        ctx.put("dq_key", dqKey);
        ctx.put("dataset_name", datasetName);
        ctx.put("spark_app_id", appId);
        return ctx;
    }

    @Override
    public void close() {
        // Manifest phai duoc ghi CA KHI mot gate da nem DqHaltException: no la thu duy nhat cho
        // agent biet pipeline co nhung checkpoint nao va schema tung diem. Mat no thi lan chay
        // sau agent van khong sinh duoc rule cho cac stage do -> HALT lap lai vinh vien.
        publishStageManifest();
        // CHI publish ban partial khi afterWrite() chua kip publish ban day du - nguoc lai
        // close() se de ban partial len tren ban hoan chinh va moi lan chay thanh cong deu bi
        // danh dau "partial: true" oan.
        if (report == null) publishPartialReport();
        publishRawEvents();   // ca khi HALT - pod Completed la mat file
        release();
    }

    /**
     * Cho listener bus xu ly het su kien da post, roi moi cho goi bo cache.
     *
     * DAY LA BAN VA CHO MOT LOI THAT, khong phai de phong xa: listener OpenLineage xu ly
     * SparkListenerSQLExecutionEnd BAT DONG BO tren AsyncEventQueue, con afterWrite() chay ngay
     * tren luong chinh sau khi lenh ghi tra ve. Bo cache truoc khi listener kip chay la go entry
     * khoi CacheManager, ma OpenLineage phai di xuyen InMemoryRelation qua
     * SharedState.cacheManager.cachedData moi lay duoc inputs va columnLineage.
     *
     * Do duoc tren cum nay TRUOC ban va: moi lan chay cua duong ghi chinh
     * (call_enricher.overwrite_partitions_dynamic...call_kpi_hourly) deu ra su kien COMPLETE
     * voi inputs=0, columnLineage=0, so cot=0 - trong khi cac lan ghi vao bang so sach cua chinh
     * DQ thi co du facet. Tuc framework dang ghi lineage cua CHINH NO chu khong phai cua pipeline.
     *
     * GOI BANG REFLECTION CO CHU Y: waitUntilEmpty la private[spark] trong Scala, chi public do
     * bytecode xoa mat pham vi. javac chiu compile, nhung mot ban Spark sau doi chu ky la
     * NoSuchMethodError giua lan chay. Reflection + catch Throwable bien chuyen do thanh mot dong
     * log, dung nguyen tac cua ca lop nay: DQ khong bao gio duoc lam hong job.
     */
    private void awaitListenerBus() {
        if (DqConfig.LISTENER_DRAIN_MS <= 0) return;
        long startedAt = System.currentTimeMillis();
        try {
            Object bus = spark.sparkContext().listenerBus();
            bus.getClass().getMethod("waitUntilEmpty", long.class)
               .invoke(bus, DqConfig.LISTENER_DRAIN_MS);
            LOG.info("=== listener bus xu ly xong sau {}ms - gio moi bo cache",
                    System.currentTimeMillis() - startedAt);
        } catch (java.lang.reflect.InvocationTargetException e) {
            // Thuong la TimeoutException: listener con dang ban (vi du OpenLineage dang POST sang
            // Marquez). Van phai bo cache - khong the giu executor mai - nhung phai noi ro, vi
            // lineage cua lan chay nay CO THE thieu facet.
            LOG.warn("=== listener bus chua xu ly xong sau {}ms ({}). Van bo cache; lineage cua lan"
                    + " chay nay co the thieu columnLineage. Nang DQ_LISTENER_DRAIN_MS neu hay gap.",
                    DqConfig.LISTENER_DRAIN_MS, String.valueOf(e.getCause()));
        } catch (Throwable t) {
            LOG.warn("=== khong cho duoc listener bus ({}) - bo cache luon nhu truoc. Lineage co the"
                    + " thieu columnLineage.", t.toString());
        }
    }

    private void release() {
        while (!gateCaches.isEmpty()) {
            try {
                gateCaches.pollFirst().unpersist();
            } catch (Exception ignored) {
                // session co the da dung
            }
        }
        if (cached == null) return;
        try {
            cached.unpersist();
        } catch (Exception ignored) {  // session co the da dung
            // khong dang de lam on
        }
        cached = null;
    }

    public static final class Builder {
        private SparkSession spark;
        private String appId;
        private String dqKey;
        private String datasetName;
        private String warehouseDb;
        private Configuration hadoopConf;
        private String hdfsBaseUri;
        private String stageView = DqLiveQuery.STAGE_VIEW;
        private final List<DqTool> extraTools = new ArrayList<>();
        // Mac dinh doc SU KIEN GOC do listener ghi ra, khong qua backend nao.
        private LineageSource lineageSource = new DqRawLineage();
        private LineageSource lineageFallback;

        public Builder spark(SparkSession v)        { this.spark = v; return this; }
        public Builder appId(String v)              { this.appId = v; return this; }
        /** Ten bang ngan gon, dung lam khoa thu muc trong /dq/. PHAI khop DQ_DATASET_NAME phia Python. */
        public Builder dqKey(String v)              { this.dqKey = v; return this; }
        public Builder datasetName(String v)        { this.datasetName = v; return this; }
        /** Database chua cac bang Iceberg cua DQ. null = khong ghi bang nao. */
        public Builder warehouse(String db)         { this.warehouseDb = db; return this; }
        public Builder hadoop(Configuration c, String baseUri) {
            this.hadoopConf = c; this.hdfsBaseUri = baseUri; return this;
        }
        /** Doi ten view agent duoc query. Mac dinh "dq_stage". */
        public Builder stageView(String v)          { this.stageView = v; return this; }
        /** Mo them kha nang cho agent o checkpoint pre_rules. */
        public Builder tool(DqTool t)               { this.extraTools.add(t); return this; }
        public Builder lineageSource(LineageSource v) { this.lineageSource = v; return this; }
        /** Duong lui khi khong co lineage goc. Chi duoc dung khi DQ_LINEAGE_MARQUEZ_FALLBACK=true. */
        public Builder lineageFallback(LineageSource v) { this.lineageFallback = v; return this; }

        public DqGuard build() {
            if (spark == null || appId == null || dqKey == null || datasetName == null) {
                throw new IllegalArgumentException(
                        "DqGuard can toi thieu: spark, appId, dqKey, datasetName");
            }
            return new DqGuard(this);
        }
    }
}
