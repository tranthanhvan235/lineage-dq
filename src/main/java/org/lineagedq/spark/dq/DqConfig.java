package org.lineagedq.spark.dq;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Moi bien moi truong cua framework DQ. dump() in ca bang ra log dau lan chay.
 * Doi ung phia Python: dq-agents/dq_env.py.
 */
public final class DqConfig {

    private static final Logger LOG = LoggerFactory.getLogger(DqConfig.class);

    private DqConfig() {
    }

    // ── Tien trinh agent ────────────────────────────────────────────────
    /** Bat/tat toan bo checkpoint. false = job chay nhu khong co DQ agent. */
    public static final boolean CHECKPOINTS_ENABLED = bool("DQ_AGENT_CHECKPOINTS", true);
    public static final String AGENT_PYTHON = str("DQ_AGENT_PYTHON", "python3");

    // ── Checkpoint theo stage (DqGuard.gate) ────────────────────────────
    /** Bat/tat toan bo gate theo stage. false = gate() tra ve df nguyen ven, khong do gi. */
    public static final boolean GATES_ENABLED = bool("DQ_GATES", true);
    /**
     * true  = HALT cua gate NEM DqHaltException, job dung that (muc dich cua gate).
     * false = HALT chi ghi log + vao report, job chay tiep.
     */
    public static final boolean GATE_ENFORCE = bool("DQ_GATE_ENFORCE", true);
    /**
     * Gate ep mot action de co so ngay tai cho. Cache dataframe de buoc sau khong phai tinh lai
     * tu dau. Giu toi da bao nhieu cache cung luc - 2 la du cho chuoi tuan tu (gate hien tai +
     * gate truoc), dat 0 de khong cache (re bo nho, dat compute).
     */
    public static final int GATE_CACHE_DEPTH = (int) num("DQ_GATE_CACHE_DEPTH", 2);
    /** never | on_violation | always - dung khi active.json khong khai policy cho stage do. */
    public static final String GATE_ASK_DEFAULT = str("DQ_GATE_ASK_DEFAULT", "on_violation");
    /** Cac stage LUON hoi agent du sach, ngan cach bang dau phay. */
    public static final java.util.Set<String> GATE_ASK_ALWAYS = new java.util.LinkedHashSet<>(
            java.util.Arrays.asList(str("DQ_GATE_ASK_ALWAYS", "").split("\\s*,\\s*")));
    /**
     * An xa cho rule set chua co verdict cua khau verify. CHI dung dung mot luot khi chuyen he:
     * moi active.json dang co tren cum deu sinh truoc khi co khau verify, khong an xa thi luot
     * ke tiep chay voi 0 rule.
     */
    public static final boolean RULES_LEGACY_AMNESTY = bool("DQ_RULES_LEGACY_AMNESTY", false);
    /**
     * CHI cho thuc nghiem (experiments/): chup du lieu tung checkpoint ra parquet tai
     * {dir}/{dqKey}/{appId}/{stage}/ de lam testbed. Rong = tat (mac dinh). Ton them mot lan
     * ghi moi checkpoint - KHONG bat o lan chay do hieu nang.
     */
    public static final String SNAPSHOT_DIR = str("DQ_SNAPSHOT_DIR", "");
    /** Ty le lay mau khi chup (1.0 = toan bo). Seed co dinh de hai lan chup cung mot mau. */
    public static final double SNAPSHOT_FRACTION = dbl("DQ_SNAPSHOT_FRACTION", 1.0);
    /** Ep package DQ ve INFO du job da dat root logger = WARN. Xem DqLogLevel. */
    public static final boolean FORCE_INFO_LOG = bool("DQ_FORCE_INFO_LOG", true);
    public static final String AGENT_DIR = str("DQ_AGENT_DIR", "/opt/dq-agents");
    public static final String AGENT_ENTRYPOINT = str("DQ_AGENT_ENTRYPOINT", "orchestrator.py");
    /** Het han thi job chay tiep (PROCEED). Executor nam khong suot quang nay. */
    /**
     * Tu ty le nay tro len, mot rule bi coi la DANG NGO: gan chac no sai gia dinh chu khong
     * phai du lieu hong.
     *
     * Co so: mot rule chat luong tot bat duoc mot phan nho du lieu le. Khi no bat TOAN BO so
     * dong thi cach giai thich don gian nhat khong phai "ca bang deu hong" ma la "dieu kien
     * viet sai". Da xay ra that ba lan trong mot lan chay: P_HOUR_VALID 500/500 (doi p_hour
     * dung 2 ky tu trong khi nguon luu "8" va chinh pipeline moi lpad no), MSISDN_VALID 200/200
     * va ORIGINAL_MSISDN_NOT_EMPTY 480/480 (bia dinh dang ^84[0-9]{8}$). Ca ba deu dung tren
     * du lieu hoan toan hop le. File business metadata cua pipeline nay da ghi san canh bao
     * dung chu ky do truoc khi no lap lai.
     *
     * Khong BO rule dang ngo khoi report - bo di thi mot bang hong that su 100% se bien mat.
     * Chi tach diem ra lam hai va bat nguoi doc nhin thay.
     */
    public static final double SUSPECT_RULE_PCT = Double.parseDouble(
            System.getenv().getOrDefault("DQ_SUSPECT_RULE_PCT", "99.0"));

    /**
     * Hanh vi mac dinh khi mot checkpoint khong dat nguong, neu policy cua stage khong khai.
     *
     * warn       ghi canh bao, chay tiep (mac dinh - khong lam phien)
     * ask_human  CHAN, cho nguoi quyet dinh
     * halt       dung ngay, khong hoi
     *
     * Mac dinh la warn co chu y: bat ask_human cho moi checkpoint thi mot luot chay co the hoi
     * chin lan, va tu lan thu ba khong ai doc nua. Dev bat o dung nhung chang dang chan.
     */
    /** 0 = KHONG gioi han, nguoi quyet dinh chu khong phai dong ho. Doi sau khi can doi lai. */
    public static final long HUMAN_WAIT_MAX_SEC = num("DQ_HUMAN_WAIT_MAX_SEC", 0);
    /** Nhip doc lai ban ghi quyet dinh tren HDFS. */
    public static final long HUMAN_WAIT_POLL_MS = num("DQ_HUMAN_WAIT_POLL_MS", 5_000);
    /** Cu bao nhieu vong thi nhac mot dong vao log - tranh ngap log khi cho lau. */
    public static final long HUMAN_WAIT_LOG_EVERY = num("DQ_HUMAN_WAIT_LOG_EVERY", 12);

    public static final String GATE_ON_FAIL_DEFAULT =
            System.getenv().getOrDefault("DQ_GATE_ON_FAIL_DEFAULT", "warn");

    public static final long AGENT_TIMEOUT_SEC = num("DQ_AGENT_TIMEOUT_SEC", 600);

    /**
     * Tran so vong lap LLM cho MOT lan goi agent, gui theo tung phien.
     *
     * 0 = khong gui gi, dich vu dung mac dinh cua no. Can bien nay vi ConfigMap dq-agent-config
     * la DUNG CHUNG cho moi pipeline: 20 vong vua du cho bang 8 cot nhung khong du cho bang 79
     * cot voi 9 checkpoint (do that: hai lan chay lien tiep cham tran 20 vong ma chua sinh xong
     * rule). Truoc day manifest co dat LLM_MAX_ITERATIONS=40 tren DRIVER, nhung o duong HTTP
     * agent chay trong pod dich vu chu khong trong driver, va runner._child_env khong ghi de
     * bien do - nen gia tri ay khong co tac dung nao. Gui theo phien la cach duy nhat de moi
     * pipeline co nguong rieng ma khong dung den ConfigMap dung chung.
     */
    public static final long AGENT_MAX_ITERATIONS = num("DQ_AGENT_MAX_ITERATIONS", 0);

    // ── Duong goi agent: ProcessBuilder (cu) hay HTTP long-poll tunnel (moi) ────
    /**
     * Java goi agent bang duong nao.
     *
     *   process_only      - y nguyen hom nay: spawn python3 orchestrator.py trong driver pod.
     *   http_only         - chi goi dich vu dq-agent-api, khong bao gio lui ve ProcessBuilder.
     *   http_then_process - MAC DINH: thu HTTP truoc, that bai SOM (chua ton mot dong ngan sach
     *                       LLM nao) thi lui ve ProcessBuilder.
     *
     * Duong ProcessBuilder KHONG BAO GIO duoc xoa: dich vu chi mot replica va trang thai phien
     * nam trong RAM cua no, nen mot lan restart giua phien la mat phien. Khi trien khai lan dau
     * nen dat DQ_AGENT_MODE=process_only trong ConfigMap, chi lat sang http_then_process sau khi
     * /v1/readyz cua dich vu da xanh.
     */
    public enum AgentMode { PROCESS_ONLY, HTTP_ONLY, HTTP_THEN_PROCESS }

    public static final AgentMode AGENT_MODE = mode("DQ_AGENT_MODE", AgentMode.HTTP_THEN_PROCESS);
    /** Base URL cua dich vu agent. ClusterIP trong ns spark, ghi FQDN de khong phu thuoc search domain. */
    public static final String AGENT_API_URL =
            str("DQ_AGENT_API_URL", "http://dq-agent-api.spark.svc.cluster.local:8080");
    /**
     * Bearer token. PHAI den tu Secret chu khong phai ConfigMap, va TUYET DOI khong bao gio
     * duoc log - khong o day, khong o DqAgentHttpClient, khong o dump().
     */
    public static final String AGENT_API_TOKEN = str("DQ_AGENT_API_TOKEN", "");

    /** connectTimeout cua HttpClient - CHI phu chang bat tay TCP, khong phu chang doc than. */
    public static final long HTTP_CONNECT_MS = num("DQ_AGENT_HTTP_CONNECT_MS", 5_000);
    /** Tran timeout cho POST /v1/sessions. */
    public static final long HTTP_OPEN_MS = num("DQ_AGENT_HTTP_OPEN_MS", 10_000);
    /** Tran wait_ms xin dich vu giu o GET /next. */
    public static final long HTTP_POLL_WAIT_MS = num("DQ_AGENT_HTTP_POLL_WAIT_MS", 20_000);
    /** Timeout cua request /next = wait_ms + phan nay (de dich vu kip tra loi truoc khi Java cat). */
    public static final long HTTP_POLL_SLACK_MS = num("DQ_AGENT_HTTP_POLL_SLACK_MS", 5_000);
    /** Tran timeout cho POST .../result. */
    public static final long HTTP_RESULT_MS = num("DQ_AGENT_HTTP_RESULT_MS", 15_000);
    /** Timeout DELETE phien - CO DINH, chay sau khi ngan sach da can nen khong duoc kep theo remaining. */
    public static final long HTTP_DELETE_MS = num("DQ_AGENT_HTTP_DELETE_MS", 2_000);
    /** Phan ngan sach de danh cho ba viec cuoi: dong registry, DELETE phien, ghi dau vet. */
    public static final long HTTP_RESERVE_MS = num("DQ_AGENT_HTTP_RESERVE_MS", 3_000);
    /**
     * Duoi nguong nay thi khong lui ve ProcessBuilder nua: khoi dong mot tien trinh agent chi de
     * giet no muoi giay sau la vo nghia, va ngan sach con lai phai du cho ca mot luot LLM.
     */
    public static final long HTTP_FALLBACK_MIN_MS = num("DQ_AGENT_HTTP_FALLBACK_MIN_MS", 60_000);
    /**
     * Tran cung so lan poll trong MOT phien, va so phong bi lien tiep khong tien trien.
     * Node nay CPU limits da overcommit 118%: mot vong lap quay tit trong driver khong chi ton
     * dien, no cuop CPU cua chinh cac thread Spark dang lam viec. Cham tran = PROCEED + dau vet,
     * de con so do nhin thay duoc trong log chu khong am tham.
     */
    public static final int HTTP_MAX_POLLS = (int) num("DQ_AGENT_HTTP_MAX_POLLS", 400);
    public static final int HTTP_MAX_NO_PROGRESS = (int) num("DQ_AGENT_HTTP_MAX_NO_PROGRESS", 200);
    /** So lan thu lai mot chang HTTP bi loi tam thoi (5xx / khong noi duoc) truoc khi bo cuoc. */
    public static final int HTTP_MAX_TRANSIENT_RETRY = (int) num("DQ_AGENT_HTTP_MAX_TRANSIENT_RETRY", 5);

    // ── Tool query du lieu dang chay ────────────────────────────────────
    public static final int LIVE_MAX_ROWS = (int) num("DQ_LIVE_MAX_ROWS", 50);
    /** Moi lan goi la mot lan Spark scan that, va job dang nam cho agent. */
    public static final int LIVE_MAX_QUERIES = (int) num("DQ_LIVE_MAX_QUERIES", 5);
    public static final String LIVE_STAGE_VIEW = str("DQ_LIVE_STAGE_VIEW", "dq_stage");

    // ── Diem chat luong ─────────────────────────────────────────────────
    // 0.9 chu khong phai 0.8: diem chat luong du lieu binh thuong rat cao, 0.9 da la mot
    // phan muoi du lieu hong - o quy mo big data do la con so lon.
    public static final double QUALITY_FAIL_THRESHOLD = dbl("DQ_QUALITY_FAIL_THRESHOLD", 0.9);
    public static final double QUALITY_WARNING_THRESHOLD = dbl("DQ_QUALITY_WARNING_THRESHOLD", 0.95);

    // ── Kho Iceberg ─────────────────────────────────────────────────────
    public static final boolean WAREHOUSE_ENABLED = bool("DQ_WAREHOUSE_ENABLED", true);
    public static final String WAREHOUSE_CATALOG = str("DQ_WAREHOUSE_CATALOG", "dq_iceberg");

    // ── Lineage ─────────────────────────────────────────────────────────
    public static final boolean LINEAGE_STORE_ENABLED = bool("DQ_LINEAGE_STORE_ENABLED", true);
    /** NDJSON do listener ghi ra (transport "file") - nguon lineage CHINH. */
    public static final String LINEAGE_RAW_FILE =
            str("DQ_LINEAGE_RAW_FILE", "/tmp/openlineage-events.ndjson");
    /** Duong lui qua REST API Marquez. Mac dinh TAT - Marquez la lop hien thi, khong phai nguon. */
    public static final boolean LINEAGE_MARQUEZ_FALLBACK = bool("DQ_LINEAGE_MARQUEZ_FALLBACK", false);
    /** So lan doi file raw co du su kien (moi lan cach LINEAGE_POLL_SLEEP_MS). */
    public static final int LINEAGE_POLL_RETRIES = (int) num("DQ_LINEAGE_POLL_RETRIES", 8);
    public static final long LINEAGE_POLL_SLEEP_MS = num("DQ_LINEAGE_POLL_SLEEP_MS", 1000);
    /** Buoc reshape facet cho Marquez UI - NGOAI flow DQ, chi de demo. */
    public static final boolean LINEAGE_RESHAPE_ENABLED = bool("DQ_LINEAGE_RESHAPE", false);
    /**
     * Cho listener bus xu ly het truoc khi bo cache, tinh bang ms. 0 = tat (hanh vi cu).
     *
     * VI SAO CAN: listener OpenLineage xu ly SparkListenerSQLExecutionEnd BAT DONG BO tren
     * AsyncEventQueue, con afterWrite() chay ngay tren luong chinh. Bo cache truoc khi listener
     * kip chay la go entry khoi CacheManager, ma OpenLineage phai di xuyen InMemoryRelation qua
     * SharedState.cacheManager.cachedData moi lay duoc inputs va columnLineage. Het entry thi
     * su kien COMPLETE ra doi voi inputs=0 va columnLineage=null - do dung la thu da xay ra tren
     * cum nay: moi lan chay cua duong ghi chinh tu 17/09 deu inputs=0, columnLineage=0.
     *
     * Vai giay cho o day KHONG lam mat y nghia cua viec bo cache som: cai dat do de tranh giu
     * executor suot AGENT_TIMEOUT_SEC cua checkpoint post_report, tinh bang phut.
     */
    public static final long LISTENER_DRAIN_MS = num("DQ_LISTENER_DRAIN_MS", 30_000);

    public static void dump() {
        LOG.info("=== DqConfig ────────────────────────────────────────");
        LOG.info("===  checkpoints={} timeout={}s python={} dir={}",
                CHECKPOINTS_ENABLED, AGENT_TIMEOUT_SEC, AGENT_PYTHON, AGENT_DIR);
        LOG.info("===  agent mode={} apiUrl={} token={}", AGENT_MODE, AGENT_API_URL,
                AGENT_API_TOKEN.isEmpty() ? "(khong dat)" : "(da dat, khong in)");
        LOG.info("===  http: connect={}ms open={}ms pollWait={}ms result={}ms reserve={}ms fallbackMin={}ms",
                HTTP_CONNECT_MS, HTTP_OPEN_MS, HTTP_POLL_WAIT_MS, HTTP_RESULT_MS,
                HTTP_RESERVE_MS, HTTP_FALLBACK_MIN_MS);
        LOG.info("===  live: view={} maxRows={} maxQueries={}",
                LIVE_STAGE_VIEW, LIVE_MAX_ROWS, LIVE_MAX_QUERIES);
        LOG.info("===  score: fail<{} warning<{}", QUALITY_FAIL_THRESHOLD, QUALITY_WARNING_THRESHOLD);
        LOG.info("===  warehouse: enabled={} catalog={}", WAREHOUSE_ENABLED, WAREHOUSE_CATALOG);
        LOG.info("===  listener drain truoc khi bo cache: {}ms", LISTENER_DRAIN_MS);
        LOG.info("===  lineage: store={} raw={} marquezFallback={} reshape={}",
                LINEAGE_STORE_ENABLED, LINEAGE_RAW_FILE, LINEAGE_MARQUEZ_FALLBACK,
                LINEAGE_RESHAPE_ENABLED);
        LOG.info("=== ──────────────────────────────────────────────────");
    }

    private static String str(String key, String fallback) {
        String v = System.getenv(key);
        return (v == null || v.isBlank()) ? fallback : v.trim();
    }

    private static boolean bool(String key, boolean fallback) {
        String v = System.getenv(key);
        if (v == null || v.isBlank()) return fallback;
        return !"false".equalsIgnoreCase(v.trim());
    }

    /**
     * PHAI nuot moi loi. Field cua lop nay la static final: mot exception luc khoi tao lop se
     * thanh ExceptionInInitializerError, va moi lan cham DqConfig sau do la NoClassDefFoundError.
     * Ca hai deu la Error chu khong phai Exception nen khong lop catch nao trong framework bat
     * duoc - mot bien go sai trong ConfigMap se GIET job.
     */
    private static AgentMode mode(String key, AgentMode fallback) {
        String v = System.getenv(key);
        if (v == null || v.isBlank()) return fallback;
        try {
            return AgentMode.valueOf(v.trim().toUpperCase(java.util.Locale.ROOT));
        } catch (RuntimeException e) {
            LOG.warn("=== DqConfig: {}='{}' khong hop le (chi nhan process_only|http_only|"
                    + "http_then_process), dung mac dinh {}", key, v, fallback);
            return fallback;
        }
    }

    private static long num(String key, long fallback) {
        try {
            return Long.parseLong(str(key, String.valueOf(fallback)));
        } catch (NumberFormatException e) {
            LOG.warn("=== DqConfig: {} khong phai so, dung mac dinh {}", key, fallback);
            return fallback;
        }
    }

    private static double dbl(String key, double fallback) {
        try {
            return Double.parseDouble(str(key, String.valueOf(fallback)));
        } catch (NumberFormatException e) {
            LOG.warn("=== DqConfig: {} khong phai so, dung mac dinh {}", key, fallback);
            return fallback;
        }
    }
}
