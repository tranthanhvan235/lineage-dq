package org.lineagedq.spark.dq;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Doc lineage tu SU KIEN GOC do OpenLineage listener ghi ra trong chinh lan chay nay.
 *
 * Khong doc Marquez vi no la lop hien thi, khong phai nguon du lieu - va endpoint
 * /api/v1/events/lineage cua no tra ve columnLineage = null (da do).
 *
 * Nguon: transport "file" cua listener, NDJSON tai DQ_LINEAGE_RAW_FILE. Cau hinh SparkApplication:
 *
 *   spark.openlineage.transport.type = composite
 *   spark.openlineage.transport.transports.rawfile.type     = file
 *   spark.openlineage.transport.transports.rawfile.location = /tmp/openlineage-events.ndjson
 *   spark.openlineage.transport.transports.marquez.type     = http   (chi de visualize)
 *
 * File nam tren o dia DRIVER, doc trong chinh lan chay - khong qua mang, khong cho ingest.
 */
public final class DqRawLineage implements DqGuard.LineageSource {

    private static final Logger LOG = LoggerFactory.getLogger(DqRawLineage.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final Path file;

    public DqRawLineage() {
        this(Paths.get(DqConfig.LINEAGE_RAW_FILE));
    }

    public DqRawLineage(Path file) {
        this.file = file;
    }

    /**
     * @return su kien COMPLETE cua lan chay nay, co facet columnLineage that cho bang dich.
     *         null khi khong tim thay sau khi da doi het luot.
     */
    @Override
    public JsonNode fetch(String appId, String outputDataset) {
        for (int lan = 0; lan < DqConfig.LINEAGE_POLL_RETRIES; lan++) {
            ObjectNode tim = scan(appId, outputDataset);
            if (tim != null) {
                LOG.info("=== DqRawLineage: lay duoc lineage goc cho {} tu {}", outputDataset, file);
                return tim;
            }
            // Listener ghi bat dong bo; lan ghi chinh vua xong nen su kien co the chua kip ra file.
            try {
                Thread.sleep(DqConfig.LINEAGE_POLL_SLEEP_MS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        diagnose(appId, outputDataset);
        return null;
    }

    /** In ra dung nhung gi co trong file khi khong tim thay - de khong phai doan roi chay lai. */
    private void diagnose(String appId, String outputDataset) {
        if (!Files.exists(file)) {
            LOG.warn("=== DqRawLineage: KHONG CO file {}. Nhanh 'file' cua transport chua duoc"
                    + " cau hinh, hoac ghi ra duong dan khac. Kiem tra"
                    + " spark.openlineage.transport.transports.<ten>.location trong SparkApplication"
                    + " va bien DQ_LINEAGE_RAW_FILE - hai cho nay phai khop.", file);
            return;
        }
        List<String> lines = readLines();
        int eventCount = 0;
        int complete = 0;
        int withFacet = 0;
        Set<String> outputNames = new LinkedHashSet<>();
        Set<String> appIds = new LinkedHashSet<>();
        for (String line : lines) {
            line = line.trim();
            if (line.isEmpty() || line.charAt(0) != '{') continue;
            JsonNode ev;
            try {
                ev = MAPPER.readTree(line);
            } catch (Exception e) {
                continue;
            }
            eventCount++;
            if ("COMPLETE".equals(ev.path("eventType").asText())) complete++;
            JsonNode id = ev.path("run").path("facets").path("spark_applicationDetails")
                    .path("applicationId");
            if (id.isTextual()) appIds.add(id.asText());
            for (JsonNode out : ev.path("outputs")) {
                String name = out.path("name").asText("");
                JsonNode fields = out.path("facets").path("columnLineage").path("fields");
                if (fields.isObject() && !fields.isEmpty()) {
                    withFacet++;
                    outputNames.add(name + " (" + fields.size() + " cot)");
                } else {
                    outputNames.add(name + " (khong co columnLineage)");
                }
            }
        }
        LOG.warn("=== DqRawLineage: file {} co {} dong / {} su kien, {} COMPLETE, {} output kem"
                + " columnLineage. Dang tim appId={} dataset={}.",
                file, lines.size(), eventCount, complete, withFacet, appId, outputDataset);
        LOG.warn("=== DqRawLineage: applicationId thay trong file: {}", appIds);
        LOG.warn("=== DqRawLineage: cac output thay trong file: {}", outputNames);
    }

    /**
     * Doc ca file, tra ve su kien mang lineage cua bang dich - UU TIEN COMPLETE nhung KHONG bat
     * buoc.
     *
     * Do tren cluster: cung mot lan chay, su kien COMPLETE cua create_table co facet
     * columnLineage RONG, con facet 8 cot that lai nam o su kien khac cua lan ghi chinh. Bat
     * buoc COMPLETE la mot rang buoc thua tu dat ra, va no lam mat sach lineage. Noi dung facet
     * khong phu thuoc eventType - cai nao mang du 'fields' thi dung duoc.
     */
    private ObjectNode scan(String appId, String outputDataset) {
        List<String> lines = readLines();
        ObjectNode preferred = null;   // tu su kien COMPLETE
        ObjectNode fallback = null;  // tu su kien bat ky
        for (String line : lines) {
            line = line.trim();
            if (line.isEmpty() || line.charAt(0) != '{') continue;
            ObjectNode ev;
            try {
                ev = (ObjectNode) MAPPER.readTree(line);
            } catch (Exception e) {  // dong hong thi bo qua, khong lam hong ca file
                continue;
            }
            if (!isSameRun(ev, appId)) continue;
            if (!hasLineageFor(ev, outputDataset)) continue;

            // Khong break: lay ban MOI NHAT trong moi nhom.
            if ("COMPLETE".equals(ev.path("eventType").asText())) {
                preferred = ev;
            } else {
                fallback = ev;
            }
        }
        if (preferred != null) return preferred;
        if (fallback != null) {
            LOG.info("=== DqRawLineage: lineage lay tu su kien {} (khong co COMPLETE nao mang"
                    + " facet day du) - noi dung facet khong phu thuoc eventType.",
                    fallback.path("eventType").asText());
        }
        return fallback;
    }

    /**
     * Khop theo applicationId khi co facet spark_applicationDetails; khong co thi CHAP NHAN, vi
     * file do chinh driver nay ghi ra nen moi su kien deu la cua lan chay nay.
     */
    private static boolean isSameRun(JsonNode ev, String appId) {
        JsonNode id = ev.path("run").path("facets").path("spark_applicationDetails")
                .path("applicationId");
        return id.isMissingNode() || id.isNull() || appId == null || appId.equals(id.asText());
    }

    /** Su kien co output dung bang dich, kem facet columnLineage co fields khong rong. */
    private static boolean hasLineageFor(JsonNode ev, String outputDataset) {
        for (JsonNode out : ev.path("outputs")) {
            String name = out.path("name").asText("");
            if (outputDataset != null && !name.equals(outputDataset) && !name.endsWith(outputDataset)) {
                continue;
            }
            JsonNode fields = out.path("facets").path("columnLineage").path("fields");
            if (fields.isObject() && !fields.isEmpty()) return true;
        }
        return false;
    }

    private List<String> readLines() {
        try {
            if (!Files.exists(file)) return List.of();
            return Files.readAllLines(file, StandardCharsets.UTF_8);
        } catch (Exception e) {  // lineage la metadata phu tro
            LOG.warn("=== DqRawLineage: khong doc duoc {}: {}", file, e.toString());
            return new ArrayList<>();
        }
    }
}
