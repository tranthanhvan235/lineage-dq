package org.lineagedq.spark.dq;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.expr;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.when;

/**
 * Do rule tai mot checkpoint bang MOT truy van aggregate RIENG, khong dung Dataset.observe().
 *
 * <h2>Vi sao khong dung observe() o checkpoint</h2>
 *
 * Do truc tiep tren Spark 4.1.2 cua cum nay (ket qua o cell-kpi-enricher/probe-observe-vs-agg.json):
 *
 * <pre>
 *   bieu thuc                      observe()                          df.agg()
 *   count(distinct k)              AGGREGATE_EXPRESSION_WITH_          OK
 *                                  DISTINCT_UNSUPPORTED
 *   sum(x) FILTER (WHERE ...)      AGGREGATE_EXPRESSION_WITH_          OK
 *                                  FILTER_UNSUPPORTED
 *   chen node vao plan cua df       CO (CollectMetrics)                KHONG
 *   pipeline se ghi tiep
 * </pre>
 *
 * Bon ly do, xep theo muc nang:
 *
 * <ol>
 *   <li><b>observe() cam count(DISTINCT).</b> Cau hoi DQ quan trong nhat cua pipeline nay -
 *       khoa groupBy 74 cot co con duy nhat sau khi explode 500 -> 1250 dong khong - khong
 *       hoi duoc bang observe. Voi agg() thi hoi duoc, va CO DIEM SO.</li>
 *   <li><b>Observation.getAsJava() la awaitResult(future, Duration.Inf).</b> Neu su kien bi
 *       AsyncEventQueue vut (queue day) thi driver treo VINH VIEN, va khong co exception nao
 *       de catch. Doi lai, agg().first() hong thi nem ngay va doc duoc.</li>
 *   <li><b>observe() chen CollectMetrics vao plan cua dataframe ma pipeline se ghi.</b> Yeu cau
 *       la DQ khong duoc anh huong pipeline goc; class nay tra df ve NGUYEN VEN.</li>
 *   <li><b>O checkpoint, observe() khong con loi the nao.</b> Loi the duy nhat cua no la di nho
 *       action co san; nhung checkpoint von da phai ep mot action de co so ngay tai cho.</li>
 * </ol>
 *
 * Doi lai: moi gate them mot Exchange(SinglePartition), va count(distinct) keo theo Expand +
 * shuffle that. Day la danh doi co loi, khong phai mien phi.
 */
public final class DqMetricQuery {

    private static final Logger LOG = LoggerFactory.getLogger(DqMetricQuery.class);

    /**
     * Ten metric dem tong so dong. DqReportWriter doc lai bang DUNG khoa nay - de no o day
     * lam mot hang so dung chung, khong viet chuoi literal o ca hai noi: lech nhau mot chu
     * thi DqReportWriter nhan 0 dong, bao INSUFFICIENT_DATA, va khong loi nao duoc nem ra.
     */
    public static final String TOTAL_ROWS_METRIC = "total_rows";

    private DqMetricQuery() {
    }

    /**
     * @param metrics       ten metric -> gia tri, dung dang DqReportWriter dang nhan
     * @param attachedRules rule thuc su do duoc (da bo cac rule khong hop schema)
     * @param droppedRules  rule bi bo kem ly do - PHAI di vao report, khong duoc chi log
     */
    public record Measured(Map<String, Object> metrics,
                           List<DqRule> attachedRules,
                           Map<String, String> droppedRules) {
    }

    /**
     * Do toan bo rule cua mot stage bang MOT action.
     *
     * Dataframe dau vao KHONG bi sua: moi thu dien ra tren mot nhanh query rieng, pipeline
     * van dung chinh df no dua vao.
     */
    public static Measured measure(Dataset<Row> df, List<DqRule> rules) {
        List<Column> aggs = new ArrayList<>();
        List<DqRule> attached = new ArrayList<>();
        Map<String, String> dropped = new LinkedHashMap<>();

        aggs.add(count(lit(1)).as(TOTAL_ROWS_METRIC));

        for (DqRule rule : rules) {
            try {
                Column metric = buildMetric(df, rule);
                aggs.add(metric);
                // Rule muc tap: do THEM gia tri that, khong chi co dung/sai. Xem
                // DqRule.observedMetricName. Loi o bieu thuc nay KHONG duoc lam rot ca rule -
                // gia tri de bao cao la thu tot-thi-co, con dung/sai moi la thu bat buoc.
                if (rule.hasObservedExpr()) {
                    try {
                        Column observed = expr(rule.observedExpr()).as(rule.observedMetricName());
                        df.agg(observed).queryExecution().analyzed();
                        aggs.add(observed);
                    } catch (Exception e) {
                        LOG.warn("=== DqMetricQuery: bo observed_expr cua '{}' - {}",
                                rule.ruleId(), shorten(e.getMessage()));
                    }
                }
                attached.add(rule);
            } catch (Exception e) {
                // Mot rule hong lam hong CA cau agg cua stage nay, nen phai loai truoc khi gop.
                // Ghi lai ly do: rule bi bo am tham tung lam stage van bao 1.0 trong khi thuc te
                // khong do gi ca.
                dropped.put(rule.ruleId(), e.getClass().getSimpleName() + ": " + shorten(e.getMessage()));
                LOG.warn("=== DqMetricQuery: bo rule '{}' (stage={}) - {}",
                        rule.ruleId(), rule.stage(), shorten(e.getMessage()));
            }
        }

        Column first = aggs.get(0);
        Column[] rest = aggs.subList(1, aggs.size()).toArray(new Column[0]);
        Row row = df.agg(first, rest).first();

        Map<String, Object> metrics = new LinkedHashMap<>();
        for (String name : row.schema().fieldNames()) {
            metrics.put(name, row.getAs(name));
        }

        LOG.info("=== DqMetricQuery: do {}/{} rule bang 1 action (bo {})",
                attached.size(), rules.size(), dropped.size());
        return new Measured(metrics, attached, dropped);
    }

    /**
     * Dung bieu thuc do cho mot rule, va PROBE no ngay de bat loi sam.
     *
     * Hai tang probe, moi tang bat mot loai loi khac:
     *   select() -> ep Catalyst resolve: cot khong ton tai, sai kieu
     *   agg()    -> bat aggregate long aggregate, window, subquery
     * Ca hai nem loi ngay luc dung Dataset, khong can action, nen gan nhu mien phi.
     */
    private static Column buildMetric(Dataset<Row> df, DqRule rule) {
        if (rule.isDatasetLevel()) {
            // Bieu thuc muc tap: CHINH no la aggregate tra ve boolean. Quy doi nhi phan sang
            // "so dong vi pham" de dung chung cong thuc diem voi rule muc dong: vi pham -> coi
            // nhu 100% dong loi, khong -> 0%.
            Column agg = expr(rule.failedSqlExpr());
            df.agg(agg.as("probe")).queryExecution().analyzed();   // probe, chua chay
            return when(agg, lit(1L)).otherwise(lit(0L)).as(rule.metricName());
        }

        Column violated = expr(rule.failedSqlExpr());
        df.select(violated);                                        // probe tang 1
        Column metric = count(when(violated, 1)).as(rule.metricName());
        df.agg(metric).queryExecution().analyzed();                 // probe tang 2
        return metric;
    }

    private static String shorten(String s) {
        if (s == null) return "(khong co thong bao)";
        String one = s.replace('\n', ' ').trim();
        return one.length() > 200 ? one.substring(0, 200) + "..." : one;
    }
}
