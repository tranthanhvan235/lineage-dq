package org.lineagedq.spark.dq.lineage;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Lay event goc do OpenLineage Spark listener sinh ra tren Marquez, DOI HINH DANG facet
 * columnLineage cho hop voi Marquez, roi emit lai.
 *
 * Truoc day class nay con bom them transformation description tu CatalystPlanParser, vi listener
 * 1.52 luon de description rong. Tu OpenLineage 1.53.0 (flag
 * spark.openlineage.columnLineage.descriptionsEnabled) listener tu sinh description bang bieu thuc
 * Catalyst that su - chinh xac hon ban tu parse (giu dung quote SQL va dung danh sach literal),
 * nen phan tu parse da bi go bo. Class nay gio CHI con lam viec tuong thich schema voi Marquez:
 *
 *   - Ban day du (1-2-0, co transformations[] + subtype + masking + mang dataset[] chua
 *     GROUP_BY/JOIN/FILTER) duoc copy sang custom facet "extendedColumnLineage" - Marquez luu
 *     custom facet nguyen ven duoi dang JSONB. dq-agents/lineage_client.py doc facet nay.
 *   - Facet "columnLineage" goc bi ha xuong dung shape 1-0-1 (bo mang transformations,
 *     day type/description len muc field) de Marquez UI ve duoc column-level lineage.
 */
public final class OpenLineageEnricher {

    private static final Logger LOG = LoggerFactory.getLogger(OpenLineageEnricher.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final String COLUMN_LINEAGE_FACET = "columnLineage";
    private static final String EXTENDED_FACET = "extendedColumnLineage";

    private final LineageConfig cfg;
    private final HttpClient http;

    public OpenLineageEnricher(LineageConfig cfg) {
        this.cfg = cfg;
        this.http = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(10))
                .build();
    }

    /**
     * Doi hinh dang facet columnLineage cho hop voi Marquez roi emit lai event.
     *
     * @param appId spark applicationId - de tim dung run cua lan chay nay
     * @return true neu da emit thanh cong
     */
    public boolean reshapeAndEmit(String appId) {
        try {
            ObjectNode event = findCompleteEvent(appId);

            if (event == null) {
                LOG.warn("===Khong tim thay COMPLETE event cua appId={} tren Marquez sau {} lan thu"
                        + " - BO QUA buoc reshape. Marquez van giu event goc do listener gui"
                        + " (facet columnLineage shape 1-2-0, Marquez UI co the khong ve duoc).",
                        appId, cfg.getMaxRetries());
                return false;
            }

            int reshaped = reshapeOutputs(event);
            if (reshaped == 0) {
                LOG.warn("===KHONG output nao co facet columnLineage de reshape. Neu lan chay nay"
                        + " that su co ghi du lieu thi day la dau hieu listener OpenLineage khong"
                        + " sinh column lineage cho write path nay - kiem tra"
                        + " spark.openlineage.columnLineage.enabled va phien ban listener.");
            }

            boolean ok = post(event);
            LOG.info("===Emit lineage da reshape: ok={} outputs={}", ok, reshaped);
            return ok;
        } catch (Exception e) {
            LOG.error("===Reshape lineage that bai: {}", e.toString(), e);
            return false;
        }
    }

    // Buoc 1: lay event goc tu Marquez

    private ObjectNode findCompleteEvent(String appId) throws Exception {
        return fetchCompleteEvent(appId, cfg.getMaxRetries());
    }

    /**
     * Lay COMPLETE event goc cua mot lan chay tu Marquez. Cong khai de buoc luu lineage xuong
     * Iceberg (dq/DqLineageWriter) dung lai dung mot ban cai dat nay, khong viet lan hai.
     *
     * @param maxRetries so lan poll. Buoc reshape chay sau spark.stop() nen du thoi gian cho;
     *                   buoc luu Iceberg chay khi executor van dang giu cho nen thuong dat thap.
     * @return null neu het luot ma khong thay
     */
    public ObjectNode fetchCompleteEvent(String appId, int maxRetries) throws Exception {
        return fetchCompleteEvent(appId, maxRetries, null);
    }

    /**
     * @param outputDataset khi khac null, CHI nhan event co output dung ten nay VA co facet
     *        columnLineage voi fields khong rong.
     *
     *        Can tham so nay vi mot lan chay sinh NHIEU event: cac lenh ghi bang thong ke DQ
     *        (dq_report_history, dq_agent_runs...) cung la Spark job nen cung duoc listener ghi
     *        nhan, va chung xay ra SAU lan ghi chinh. Lay "event moi nhat co columnLineage" la
     *        vo phai chung - da xay ra that: dq_column_lineage nhan 0 dong vi bat duoc event
     *        create_table cua mot bang DQ, ma event do co facet nhung fields rong.
     *
     *        Khi loc theo ten thi KHONG co duong lui "chap nhan event khong co columnLineage o
     *        lan thu cuoi": luu lineage cua sai dataset con te hon khong luu gi.
     */
    public ObjectNode fetchCompleteEvent(String appId, int maxRetries, String outputDataset)
            throws Exception {
        for (int attempt = 0; attempt < maxRetries; attempt++) {
            List<JsonNode> events = fetchEventPages(attempt);
            if (events.isEmpty()) {
                Thread.sleep(cfg.getRetrySleepMs());
                continue;
            }

            // 1a. Tim tat ca runId cua START event khop applicationId
            Set<String> targetRunIds = new LinkedHashSet<>();
            for (JsonNode ev : events) {
                if (!"START".equals(ev.path("eventType").asText())) continue;
                String evAppId = ev.path("run").path("facets")
                        .path("spark_applicationDetails").path("applicationId").asText("");
                if (appId.equals(evAppId)) {
                    targetRunIds.add(ev.path("run").path("runId").asText());
                }
            }

            if (targetRunIds.isEmpty()) {
                Thread.sleep(cfg.getRetrySleepMs());
                continue;
            }

            // 1b. Uu tien COMPLETE event vua co outputs vua co san facet columnLineage
            ObjectNode withColumnLineage = null;
            ObjectNode withOutputs = null;
            for (JsonNode ev : events) {
                if (!"COMPLETE".equals(ev.path("eventType").asText())) continue;
                if (!targetRunIds.contains(ev.path("run").path("runId").asText())) continue;
                JsonNode outputs = ev.path("outputs");
                if (!outputs.isArray() || outputs.isEmpty()) continue;

                if (withOutputs == null && outputDataset == null) {
                    withOutputs = (ObjectNode) ev.deepCopy();
                }
                for (JsonNode out : outputs) {
                    if (!matches(out, outputDataset)) continue;
                    withColumnLineage = (ObjectNode) ev.deepCopy();
                    break;
                }
                if (withColumnLineage != null) break;
            }

            if (withColumnLineage != null) return withColumnLineage;

            // Chi chap nhan event khong co columnLineage o lan thu cuoi. Neu nhan som,
            // rat de vo phai COMPLETE cua create_table (cung co outputs nhung khong co
            // column lineage) trong khi event overwrite_partitions chua kip len Marquez.
            if (withOutputs != null && attempt == maxRetries - 1) {
                LOG.warn("===Khong co COMPLETE event nao kem facet columnLineage, "
                        + "dung event {} va tu dung facet tu Catalyst plan",
                        withOutputs.path("job").path("name").asText());
                return withOutputs;
            }

            Thread.sleep(cfg.getRetrySleepMs());
        }
        return null;
    }

    /**
     * Doc toi da {@code eventsMaxPages} trang event, moi trang {@code eventsLimit} ban ghi.
     *
     * Chia nho trang la BAT BUOC chu khong phai toi uu: endpoint /api/v1/events/lineage khong
     * loc theo namespace va moi event Spark nang ~150KB (schema + plan facet), nen xin mot lan
     * 100 event lam marquez-api het heap va tra HTTP 500. Trang nao loi thi dung o do va dung
     * nhung trang da doc duoc - van co the du de tim thay event can.
     */
    private List<JsonNode> fetchEventPages(int attempt) {
        List<JsonNode> all = new ArrayList<>();
        for (int page = 0; page < cfg.getEventsMaxPages(); page++) {
            JsonNode data;
            try {
                data = MAPPER.readTree(get(cfg.getEventsUrl(page)));
            } catch (Exception e) {
                LOG.debug("Lan {} trang {}: chua doc duoc events tu Marquez: {}",
                        attempt, page, e.toString());
                break;
            }
            JsonNode events = data.path("events");
            if (!events.isArray() || events.isEmpty()) break;
            events.forEach(all::add);
            // Trang cuoi (it hon mot trang day) -> khong con gi de doc
            if (events.size() < cfg.getEventsLimit()) break;
        }
        return all;
    }

    /**
     * @return true neu output nay dung la thu dang tim: co facet columnLineage; khi co yeu cau
     *         ten thi phai khop ten; va fields phai khong rong - event create_table co facet
     *         nhung fields rong, lay no ve la duoc mot ban ghi trong ruot.
     */
    private static boolean matches(JsonNode output, String outputDataset) {
        JsonNode cl = output.path("facets").path(COLUMN_LINEAGE_FACET);
        if (cl.isMissingNode() || cl.isNull()) return false;
        if (outputDataset == null) return true;

        String name = output.path("name").asText("");
        if (!name.equals(outputDataset) && !name.endsWith(outputDataset)) return false;
        return cl.path("fields").isObject() && !cl.path("fields").isEmpty();
    }

    /**
     * Lay tai lieu lineage DAY DU NHAT cho mot lan chay, de luu xuong Iceberg.
     *
     * Phai gop HAI endpoint cua Marquez, vi khong cai nao du mot minh:
     *   /api/v1/events/lineage   co metadata lan chay (runId, job, thoi diem, inputs/outputs)
     *                            NHUNG tra ve columnLineage = null. Da do that: facet co mat
     *                            trong danh sach khoa, gia tri la null.
     *   /api/v1/namespaces/.../datasets/...  co columnLineage THAT (fields + dataset[]), nhung
     *                            la trang thai HIEN TAI cua dataset, khong gan voi lan chay nao.
     *
     * O day lay event cua dung lan chay roi CAM facet that vao output tuong ung. Ket qua la mot
     * event hoan chinh: DqLineageWriter lam phang no nhu binh thuong, va raw_event_json luu lai
     * duoc ca hai nua.
     *
     * Day cung la endpoint dq-agents/lineage_client.py doc, nen lineage luu xuong bang khop voi
     * thu agent nhin thay - neu lay tu cho khac thi hai ben co the ke hai chuyen khac nhau.
     *
     * @return null neu khong tim duoc event cua lan chay nay
     */
    public ObjectNode fetchLineageSnapshot(String appId, String datasetName, int maxRetries)
            throws Exception {
        ObjectNode event = fetchCompleteEvent(appId, maxRetries, null);
        if (event == null) return null;

        JsonNode facet = fetchDatasetColumnLineage(datasetName);
        if (facet == null || facet.isNull()) {
            LOG.warn("===Dataset {} chua co facet columnLineage tren Marquez - luu event khong"
                    + " kem lineage cot.", datasetName);
            return event;
        }

        int injected = 0;
        for (JsonNode outNode : event.path("outputs")) {
            String name = outNode.path("name").asText("");
            if (!name.equals(datasetName) && !name.endsWith(datasetName)) continue;
            childObject((ObjectNode) outNode, "facets").set(COLUMN_LINEAGE_FACET, facet.deepCopy());
            injected++;
        }
        if (injected == 0) {
            // Event cua lan chay nay khong ghi vao dataset do. Tu them mot output de khong mat
            // lineage, thay vi tra ve mot event ma DqLineageWriter se lam phang thanh 0 dong.
            ObjectNode out = ((com.fasterxml.jackson.databind.node.ArrayNode)
                    event.withArray("outputs")).addObject();
            out.put("namespace", cfg.getDatasetNamespace());
            out.put("name", datasetName);
            out.putObject("facets").set(COLUMN_LINEAGE_FACET, facet.deepCopy());
            LOG.info("===Event khong co output {} - them vao de giu lineage cot.", datasetName);
        }
        return event;
    }

    /** @return facet columnLineage hien tai cua dataset, hoac null. */
    private JsonNode fetchDatasetColumnLineage(String datasetName) {
        String url = cfg.getApiBase() + "/api/v1/namespaces/"
                + URLEncoder.encode(cfg.getDatasetNamespace(), StandardCharsets.UTF_8)
                + "/datasets/" + URLEncoder.encode(datasetName, StandardCharsets.UTF_8);
        try {
            JsonNode ds = MAPPER.readTree(get(url));
            JsonNode cl = ds.path("facets").path(COLUMN_LINEAGE_FACET);
            return cl.isMissingNode() ? null : cl;
        } catch (Exception e) {  // lineage la metadata phu tro
            LOG.warn("===Khong doc duoc facet columnLineage tu {}: {}", url, e.toString());
            return null;
        }
    }

    // Buoc 2: giu ban day du o custom facet + ha facet goc xuong 1-0-1

    /** @return so output dataset thuc su co facet columnLineage de reshape. */
    private int reshapeOutputs(ObjectNode event) {
        int reshaped = 0;
        JsonNode outputs = event.path("outputs");
        if (!outputs.isArray()) return 0;

        for (JsonNode outNode : outputs) {
            ObjectNode output = (ObjectNode) outNode;
            ObjectNode facets = childObject(output, "facets");

            if (!(facets.get(COLUMN_LINEAGE_FACET) instanceof ObjectNode colLineage)) {
                // Listener khong sinh columnLineage cho output nay. Truoc day job tu dung facet
                // tu CatalystPlanParser; tu khi bo buoc do thi khong con gi de dung thay - chi
                // canh bao that ro thay vi im lang bo qua.
                LOG.warn("===Output {} KHONG co facet columnLineage tu listener - khong co gi de"
                        + " reshape cho output nay.", output.path("name").asText());
                continue;
            }

            // 2a. Giu ban day du (1-2-0: subtype, masking, transformations[], dataset[] chua
            // GROUP_BY/JOIN/FILTER) o custom facet - dq-agents/lineage_client.py doc facet nay.
            facets.set(EXTENDED_FACET, colLineage.deepCopy());

            // 2b. Ha facet goc ve dung shape 1-0-1 cho Marquez UI
            downgradeToV101(childObject(colLineage, "fields"));
            reshaped++;
        }
        return reshaped;
    }

    /** Bo mang transformations, day type/description len muc field theo spec 1-0-1. */
    private void downgradeToV101(ObjectNode fields) {
        List<String> names = new ArrayList<>();
        fields.fieldNames().forEachRemaining(names::add);

        for (String fieldName : names) {
            ObjectNode field = (ObjectNode) fields.get(fieldName);
            JsonNode inputFields = field.path("inputFields");
            ObjectNode firstTransformation = null;

            for (JsonNode inputFieldNode : inputFields) {
                ObjectNode inputField = (ObjectNode) inputFieldNode;
                JsonNode transformations = inputField.path("transformations");
                if (firstTransformation == null && transformations.isArray() && !transformations.isEmpty()) {
                    firstTransformation = (ObjectNode) transformations.get(0).deepCopy();
                }
                inputField.remove("transformations");
            }

            if (firstTransformation != null) {
                field.put("transformationType", firstTransformation.path("type").asText(""));
                field.put("transformationDescription", firstTransformation.path("description").asText(""));
            }
        }
    }

    // HTTP

    private String get(String url) throws Exception {
        HttpRequest request = HttpRequest.newBuilder(URI.create(url))
                .timeout(Duration.ofSeconds(15))
                .GET()
                .build();
        HttpResponse<String> response = http.send(request, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() / 100 != 2) {
            throw new IllegalStateException("GET " + url + " -> HTTP " + response.statusCode());
        }
        return response.body();
    }

    private boolean post(ObjectNode event) throws Exception {
        byte[] payload = MAPPER.writeValueAsBytes(event);
        HttpRequest request = HttpRequest.newBuilder(URI.create(cfg.getLineageUrl()))
                .timeout(Duration.ofSeconds(15))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofByteArray(payload))
                .build();
        HttpResponse<String> response = http.send(request, HttpResponse.BodyHandlers.ofString());
        int status = response.statusCode();
        if (status / 100 == 2) return true;

        LOG.error("===POST lineage that bai: HTTP {} body={}", status,
                new String(response.body().getBytes(StandardCharsets.UTF_8)));
        return false;
    }

    private static ObjectNode childObject(ObjectNode parent, String field) {
        JsonNode existing = parent.get(field);
        if (existing instanceof ObjectNode node) return node;
        return parent.putObject(field);
    }
}
