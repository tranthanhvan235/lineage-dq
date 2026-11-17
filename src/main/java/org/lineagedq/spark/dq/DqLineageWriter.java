package org.lineagedq.spark.dq;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Luu lineage xuong Iceberg: Postgres cua Marquez khong phai cho de scan lich su lineage cua
 * hang tram bang qua hang nghin lan chay, Iceberg thi Trino doc duoc.
 *
 * Marquez van giu vai tro visualize - day la noi luu ben lau, khong phai ban thay the.
 */
public final class DqLineageWriter {

    private static final Logger LOG = LoggerFactory.getLogger(DqLineageWriter.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    /** Facet goc do listener sinh (shape 1-2-0). Buoc reshape co the copy no sang facet duoi. */
    private static final String COLUMN_LINEAGE = "columnLineage";
    private static final String EXTENDED = "extendedColumnLineage";

    private DqLineageWriter() {
    }

    /**
     * @param event COMPLETE event lay tu Marquez (OpenLineageEnricher.fetchCompleteEvent).
     *              null thi khong ghi gi - lan chay do khong co lineage de luu.
     */
    public static void append(IcebergSink sink, String dqKey, String runId, JsonNode event) {
        if (event == null) {
            LOG.warn("=== DqLineageWriter: khong co event lineage cho run {} - bo qua. Lineage van"
                    + " nam tren Marquez, chi la khong duoc luu xuong Iceberg lan nay.", runId);
            return;
        }
        // Idempotent theo run_id (count() tra -1 khi khong doc duoc thi van ghi).
        if (sink.count(DqTables.LINEAGE_RUNS, "run_id = " + IcebergSink.lit(runId)) > 0) {
            LOG.info("=== DqLineageWriter: run {} da co - bo qua", runId);
            return;
        }

        String capturedAt = Instant.now().toString();
        List<Row> colRows = new ArrayList<>();
        for (JsonNode output : event.path("outputs")) {
            JsonNode facets = output.path("facets");
            // Uu tien facet goc; extendedColumnLineage chi la ban copy cua buoc reshape.
            JsonNode cl = facets.has(COLUMN_LINEAGE) ? facets.path(COLUMN_LINEAGE)
                    : facets.path(EXTENDED);
            if (cl.isMissingNode() || cl.isNull()) continue;

            String outNs = output.path("namespace").asText(null);
            String outDs = output.path("name").asText(null);

            // 1. Phu thuoc TRUC TIEP: fields.{ten cot}.inputFields[]
            JsonNode fields = cl.path("fields");
            Iterator<String> fieldNames = fields.fieldNames();
            while (fieldNames.hasNext()) {
                String field = fieldNames.next();
                for (JsonNode in : fields.path(field).path("inputFields")) {
                    addRows(colRows, runId, dqKey, "FIELD", outNs, outDs, field, in, capturedAt);
                }
            }
            // 2. Phu thuoc GIAN TIEP: dataset[] (GROUP_BY / JOIN / FILTER)
            for (JsonNode in : cl.path("dataset")) {
                addRows(colRows, runId, dqKey, "DATASET", outNs, outDs, null, in, capturedAt);
            }
        }

        String rawJson;
        try {
            rawJson = MAPPER.writeValueAsString(event);
        } catch (Exception e) {
            rawJson = null;
        }

        sink.append(DqTables.LINEAGE_RUNS, List.of(RowFactory.create(
                runId,
                event.path("run").path("runId").asText(null),
                event.path("job").path("namespace").asText(null),
                event.path("job").path("name").asText(null),
                event.path("eventType").asText(null),
                event.path("eventTime").asText(null),
                dqKey,
                datasetNames(event.path("inputs")),
                datasetNames(event.path("outputs")),
                rawJson,
                capturedAt)));
        if (colRows.isEmpty()) {
            LOG.warn("=== DqLineageWriter: event cua run {} KHONG co column lineage nao - chi luu"
                    + " dong run. Kiem tra listener co bat duoc lan ghi chinh khong.", runId);
        }
        sink.append(DqTables.COLUMN_LINEAGE, colRows);
    }

    /** Mot cap (field dich, field nguon). `in` la phan tu cua inputFields[] hoac dataset[]. */
    private static void addRows(List<Row> rows, String runId, String dqKey, String scope,
                                 String outNs, String outDs, String outField,
                                 JsonNode in, String capturedAt) {
        JsonNode trans = in.path("transformations");
        // Mot cap field co the mang NHIEU transformation - lam phang het, chon mot la mat thong tin.
        if (trans.isArray() && !trans.isEmpty()) {
            for (JsonNode t : trans) {
                rows.add(RowFactory.create(runId, dqKey, scope, outNs, outDs, outField,
                        in.path("namespace").asText(null), in.path("name").asText(null),
                        in.path("field").asText(null),
                        t.path("type").asText(null), t.path("subtype").asText(null),
                        t.path("description").asText(null),
                        t.hasNonNull("masking") ? t.path("masking").asBoolean() : null,
                        capturedAt));
            }
            return;
        }
        // Shape 1-0-1 (da bi ha cap): type/description nam thang tren field, khong co mang.
        rows.add(RowFactory.create(runId, dqKey, scope, outNs, outDs, outField,
                in.path("namespace").asText(null), in.path("name").asText(null),
                in.path("field").asText(null),
                null, null, null, null, capturedAt));
    }

    private static String datasetNames(JsonNode arr) {
        List<String> names = new ArrayList<>();
        for (JsonNode n : arr) names.add(n.path("name").asText(""));
        return String.join(",", names);
    }
}
