package org.lineagedq.spark.dq;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Tool cho agent chay SELECT tren du lieu TRUNG GIAN ma job dang giu trong bo nho.
 *
 * Metric cua rule chi la mot dong aggregate, khong cho xem dong vi pham hay so sanh giua cac
 * dong. Qua duong nay agent chay SQL day du tren du lieu chua ghi ra dau.
 *
 * Gioi han (agent la LLM, khong tin tuyet doi duoc):
 *   - CHI cho SELECT / WITH. Moi cau khac bi tu choi thang.
 *   - Chan cac tu khoa ghi/xoa du ngay ca khi nam giua cau.
 *   - CHI duoc doc view duoc dang ky. Framework nay kiem tra DQ tren MOT bang: tap rule sinh ra
 *     tuong ung tung bang, nen mot cau query dung sang bang khac la ra ngoai pham vi thiet ke.
 *   - Cat ket qua de mot cau SELECT * khong keo ca bang ve driver.
 *   - Gioi han so lan goi moi checkpoint - moi lan la mot lan Spark scan that.
 */
public final class DqLiveQuery implements DqTool {

    private static final Logger LOG = LoggerFactory.getLogger(DqLiveQuery.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    /** Ten view mac dinh ma job dang ky cho Dataset trung gian. */
    public static final String STAGE_VIEW = DqConfig.LIVE_STAGE_VIEW;

    private static final String[] CAM = {
            "INSERT ", "UPDATE ", "DELETE ", "DROP ", "ALTER ", "TRUNCATE ", "CREATE ",
            "MERGE ", "REPLACE ", "GRANT ", "REVOKE ", "CALL ", ";",
    };

    private final SparkSession spark;
    private final String view;
    private int callCount = 0;

    public DqLiveQuery(SparkSession spark) {
        this(spark, STAGE_VIEW);
    }

    public DqLiveQuery(SparkSession spark, String view) {
        this.spark = spark;
        this.view = view;
    }

    @Override
    public String name() {
        return "query_live_stage";
    }

    /** Mo ta di thang vao prompt cua agent qua dong hello - de o day vi job biet ro nhat. */
    @Override
    public String description() {
        return "Chay SELECT tren du lieu TRUNG GIAN job Spark dang giu trong bo nho (view "
                + view + "). Du lieu nay CHUA ghi ra dau ca va se bien mat khi job ket thuc - "
                + "day la co hoi duy nhat nhin vao no.\n"
                + "Dung khi metric cua rule KHONG du de ket luan: metric chi la mot con so "
                + "tong hop, khong cho xem dong vi pham, toan ven tham chieu hay so sanh giua "
                + "cac dong.\n"
                + "Nen chay it nhat MOT cau kiem tra cau truc ma bo rule khong the kiem - dien "
                + "hinh la tinh duy nhat cua khoa group-by:\n"
                + "    SELECT count(*) AS tong, count(DISTINCT <cac cot khoa>) AS khoa_rieng FROM "
                + view + "\n"
                + "Neu tong != khoa_rieng thi du lieu co dong trung khoa - loi nghiem trong ma "
                + "MOI rule deu mu tit, vi failed_sql_expr chi soi duoc tung dong mot. Phat hien "
                + "trung thi neu ro trong reason cua checkpoint_decision.\n"
                + "CHI duoc SELECT, va CHI duoc doc view " + view + " (framework kiem DQ tren MOT "
                + "bang). Toi da " + DqConfig.LIVE_MAX_QUERIES + " lan goi, moi lan tra toi da "
                + DqConfig.LIVE_MAX_ROWS + " dong.";
    }

    @Override
    public ObjectNode parameters() {
        ObjectNode params = MAPPER.createObjectNode();
        params.put("type", "object");
        ObjectNode props = params.putObject("properties");
        props.putObject("sql")
                .put("type", "string")
                .put("description", "Cau SELECT tren view " + view + ".");
        params.putArray("required").add("sql");
        return params;
    }

    @Override
    public JsonNode execute(JsonNode args) throws Exception {
        if (++callCount > DqConfig.LIVE_MAX_QUERIES) {
            throw new IllegalStateException("Da dung het " + DqConfig.LIVE_MAX_QUERIES
                    + " luot query cho checkpoint nay. Hay ket luan bang du lieu da co.");
        }

        String sql = args.path("sql").asText("").trim();
        String reason = validate(sql, view);
        if (reason != null) {
            LOG.warn("=== DqLiveQuery: TU CHOI cau SQL cua agent: {} | sql={}", reason, sql);
            throw new IllegalArgumentException(reason);
        }

        LOG.info("=== DqLiveQuery [{}/{}]: {}", callCount, DqConfig.LIVE_MAX_QUERIES, sql);
        long startedAt = System.currentTimeMillis();
        List<Row> rows = spark.sql(sql).limit(DqConfig.LIVE_MAX_ROWS + 1).collectAsList();
        boolean truncated = rows.size() > DqConfig.LIVE_MAX_ROWS;
        if (truncated) rows = rows.subList(0, DqConfig.LIVE_MAX_ROWS);

        ObjectNode result = MAPPER.createObjectNode();
        ArrayNode cols = result.putArray("columns");
        if (!rows.isEmpty()) {
            for (String c : rows.get(0).schema().fieldNames()) cols.add(c);
        }
        ArrayNode data = result.putArray("rows");
        for (Row r : rows) {
            ArrayNode dong = data.addArray();
            for (int i = 0; i < r.length(); i++) {
                Object v = r.get(i);
                dong.add(v == null ? null : String.valueOf(v));
            }
        }
        result.put("row_count", rows.size());
        result.put("truncated", truncated);
        result.put("took_ms", System.currentTimeMillis() - startedAt);
        LOG.info("=== DqLiveQuery: tra ve {} dong trong {}ms", rows.size(),
                result.path("took_ms").asLong());
        return result;
    }

    /** Ten bang/view dung sau FROM hoac JOIN. */
    private static final Pattern NGUON = Pattern.compile(
            "(?i)\\b(?:FROM|JOIN)\\s+([A-Za-z_][A-Za-z0-9_.`]*)");

    /** Ten CTE khai bao bang "WITH x AS (" hoac ", y AS (". */
    private static final Pattern CTE = Pattern.compile(
            "(?i)(?:\\bWITH\\s+|,\\s*)([A-Za-z_][A-Za-z0-9_]*)\\s+AS\\s*\\(");

    /** @return null neu hop le, nguoc lai la ly do tu choi. */
    static String validate(String sql, String view) {
        if (sql.isEmpty()) return "Cau SQL rong.";
        String up = " " + sql.toUpperCase(Locale.ROOT) + " ";
        if (!(up.trim().startsWith("SELECT") || up.trim().startsWith("WITH"))) {
            return "Chi cho phep SELECT (hoac WITH ... SELECT).";
        }
        for (String tu : CAM) {
            if (up.contains(tu)) return "Cau SQL chua tu khoa khong duoc phep: " + tu.trim();
        }

        // CTE do chinh cau nay khai bao thi duoc phep - noi dung CTE cung bi vong lap nay kiem.
        Set<String> allowed = new HashSet<>();
        allowed.add(view.toLowerCase(Locale.ROOT));
        Matcher cte = CTE.matcher(sql);
        while (cte.find()) {
            allowed.add(cte.group(1).toLowerCase(Locale.ROOT));
        }

        Matcher m = NGUON.matcher(sql);
        while (m.find()) {
            String source = m.group(1).replace("`", "").toLowerCase(Locale.ROOT);
            if (!allowed.contains(source)) {
                return "Chi duoc truy van view '" + view + "' (framework kiem tra DQ tren "
                        + "MOT bang). Cau nay dung toi: " + m.group(1);
            }
        }
        return null;
    }
}
