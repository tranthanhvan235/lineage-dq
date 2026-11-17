package org.lineagedq.spark.dq.lineage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Cau hinh cho buoc reshape lineage. Doc tu env de khong phai sua jar khi doi cluster.
 *
 * Cac bien:
 *   OPENLINEAGE_URL       endpoint POST cua Marquez (mac dinh: marquez-api trong namespace marquez)
 *   LINEAGE_MAX_RETRIES   so lan poll raw event tu Marquez (mac dinh 15, moi lan cach 2s)
 *   LINEAGE_RETRY_SLEEP_MS khoang cach giua cac lan poll
 *   LINEAGE_EVENTS_LIMIT     so event moi trang khi GET /api/v1/events/lineage (mac dinh 20)
 *   LINEAGE_EVENTS_MAX_PAGES so trang doc toi da moi lan poll (mac dinh 5)
 *
 * Vi sao phai phan trang: endpoint /api/v1/events/lineage khong loc theo namespace, va moi
 * event Spark keo theo schema + plan facet nen nang ~150KB. Doc 100 event mot lan lam
 * marquez-api het heap va tra HTTP 500 ("Ran out of memory retrieving query results"), tuc
 * buoc doc lineage khong bao gio tim thay event. Do tren lab: limit=20 tra ~2.7MB chay tot,
 * limit=50 da 500.
 *
 * OPENLINEAGE_NAMESPACE / OPENLINEAGE_PRODUCER / LINEAGE_SYNTHETIC_FALLBACK da bo: job khong con
 * tu dung facet hay tu phat event nua, no chi doi hinh dang facet cua chinh listener roi gui lai.
 */
public final class LineageConfig {

    private static final Logger LOG = LoggerFactory.getLogger(LineageConfig.class);

    private static final String DEFAULT_URL =
            "http://marquez-api.marquez.svc.cluster.local:5000/api/v1/lineage";

    private final String lineageUrl;
    private final String eventsBaseUrl;
    private final String apiBase;
    private final String datasetNamespace;
    private final int maxRetries;
    private final long retrySleepMs;
    private final int eventsLimit;
    private final int eventsMaxPages;

    private LineageConfig(String lineageUrl, int maxRetries, long retrySleepMs,
                          int eventsLimit, int eventsMaxPages) {
        this.lineageUrl = lineageUrl;
        this.eventsBaseUrl = lineageUrl.replace("/api/v1/lineage", "/api/v1/events/lineage");
        this.apiBase = lineageUrl.replace("/api/v1/lineage", "");
        this.eventsLimit = eventsLimit;
        this.eventsMaxPages = eventsMaxPages;
        // Namespace cua DATASET (khac namespace cua JOB): chinh la URI kho vat ly.
        // dq-agents/lineage_client.py doc cung bien nay - hai ben phai tro cung mot cho.
        this.datasetNamespace = env("OPENLINEAGE_DATASET_NAMESPACE",
                "hdfs://namenode:9000");
        this.maxRetries = maxRetries;
        this.retrySleepMs = retrySleepMs;
    }

    public static LineageConfig fromEnv() {
        LineageConfig cfg = new LineageConfig(
                env("OPENLINEAGE_URL", DEFAULT_URL),
                intEnv("LINEAGE_MAX_RETRIES", 15),
                intEnv("LINEAGE_RETRY_SLEEP_MS", 2000),
                intEnv("LINEAGE_EVENTS_LIMIT", 20),
                intEnv("LINEAGE_EVENTS_MAX_PAGES", 5));
        LOG.info("===LineageConfig url={} events={} limit={} maxPages={}",
                cfg.lineageUrl, cfg.eventsBaseUrl, cfg.eventsLimit, cfg.eventsMaxPages);
        return cfg;
    }

    private static String env(String key, String def) {
        String v = System.getenv(key);
        return (v == null || v.isBlank()) ? def : v.trim();
    }

    private static int intEnv(String key, int def) {
        try {
            return Integer.parseInt(env(key, String.valueOf(def)));
        } catch (NumberFormatException e) {
            return def;
        }
    }

    public String getLineageUrl()      { return lineageUrl; }
    public String getApiBase()         { return apiBase; }
    public String getDatasetNamespace() { return datasetNamespace; }
    /** URL doc mot trang event, moi nhat truoc (Marquez sap xep event_time DESC). */
    public String getEventsUrl(int page) {
        return eventsBaseUrl + "?limit=" + eventsLimit + "&offset=" + (page * eventsLimit);
    }

    public int getEventsLimit()        { return eventsLimit; }
    public int getEventsMaxPages()     { return eventsMaxPages; }
    public int getMaxRetries()         { return maxRetries; }
    public long getRetrySleepMs()      { return retrySleepMs; }
}
