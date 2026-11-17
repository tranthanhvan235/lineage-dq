package org.lineagedq.spark.dq;

/**
 * Mot Data Quality rule sinh ra boi Gen Rules agent (Python, dq-agents/gen_rules_agent.py).
 * Cung schema voi rules.json ma agent do luu: stage, rule_id, column, description,
 * failed_sql_expr, importance_weight. failed_sql_expr la inverted logic (bieu thuc SQL tim
 * DONG LOI), phai bay duoc NULL - xem dq-agents/skills/dq-rule-generator/OUTPUT_FORMAT.md.
 *
 * <b>stage</b> la diem checkpoint ma rule nay duoc do. Cung mot cot co the co rule KHAC NHAU o
 * cac stage khac nhau, va do la co y chu khong phai trung lap - vi phep bien doi giua hai
 * stage lam doi ca gia tri lan so dong:
 *
 *   - doi GIA TRI: end_province sau coalesce(city_code,'unknown') khong bao gio NULL nua, nen
 *     rule "IS NULL" dat sau coalesce la rule chet; dat truoc coalesce moi bat duoc join truot.
 *   - doi SO DONG: do tren pipeline nay, 500 dong nguon -> 1250 dong sau explode -> 500 dong
 *     sau groupBy. Cung mot bieu thuc cho ba con so vi pham khac nhau.
 *
 * Vi vay khoa dinh danh cua rule la (stage, ruleId), khong phai ruleId - va diem chat luong
 * tinh RIENG tung stage, khong gop.
 */
public record DqRule(
        String stage,
        String ruleId,
        String column,
        String description,
        String failedSqlExpr,
        int importanceWeight,
        String kind,
        String observedExpr,
        String expected,
        String observedUnit) {

    /** Stage mac dinh cho rule cu chua khai stage: do o ngay truoc lenh ghi (hanh vi cu). */
    public static final String STAGE_FINAL = "final";

    /** Bieu thuc MUC DONG: dem dong vi pham bang count(when(expr,1)). */
    public static final String KIND_ROW = "row";
    /**
     * Bieu thuc MUC TAP: chinh no la mot aggregate tra ve boolean, vi du
     * count(distinct call_id) &lt;&gt; count(call_id). Khong co ty le nen quy doi NHI PHAN:
     * vi pham -> 100%, khong -> 0%.
     *
     * Do bang df.agg() trong DqMetricQuery, nen count(DISTINCT) dung duoc.
     */
    public static final String KIND_DATASET = "dataset";

    /**
     * Don vi cua observedExpr. Quyet dinh cach quy doi rule muc tap ra ty le vi pham.
     *
     * Khong the suy tu bieu thuc: `count(k) - count(DISTINCT k)` la SO DONG, con
     * `count(*) FILTER (...) * 100.0 / count(*)` da la PHAN TRAM, va `avg(duration)` la mili
     * giay. Chia bua cho total_rows thi hai cai sau ra so vo nghia. Nen rule phai tu khai.
     */
    public static final String UNIT_ROWS = "rows";
    public static final String UNIT_PERCENT = "percent";
    /** Khong quy doi duoc (vd avg, max) - giu quy uoc nhi phan 0%/100%. */
    public static final String UNIT_VALUE = "value";

    public DqRule {
        if (stage == null || stage.isBlank()) stage = STAGE_FINAL;
        if (kind == null || kind.isBlank()) kind = KIND_ROW;
        if (observedUnit == null || observedUnit.isBlank()) observedUnit = UNIT_VALUE;
    }

    /** Ban rut gon cho rule muc dong - giu cho cho goi cu khong phai sua. */
    public DqRule(String stage, String ruleId, String column, String description,
                  String failedSqlExpr, int importanceWeight) {
        this(stage, ruleId, column, description, failedSqlExpr, importanceWeight, KIND_ROW,
                null, null, UNIT_VALUE);
    }

    public DqRule(String stage, String ruleId, String column, String description,
                  String failedSqlExpr, int importanceWeight, String kind) {
        this(stage, ruleId, column, description, failedSqlExpr, importanceWeight, kind,
                null, null, UNIT_VALUE);
    }

    public boolean isDatasetLevel() {
        return KIND_DATASET.equals(kind);
    }

    /** Co gia tri quan sat de bao cao khong - chi rule muc tap moi can. */
    public boolean hasObservedExpr() {
        return observedExpr != null && !observedExpr.isBlank();
    }

    /**
     * Ten metric cho GIA TRI quan sat, tach khoi metric dung/sai.
     *
     * Vi sao can: voi rule muc tap, failed_sql_expr la mot BOOLEAN (vi du MAX(cnt) > 1) nen
     * ket qua do chi con 1 hoac 0 - biet la hong nhung khong biet hong bao nhieu. Doc report
     * thay "vi pham" ma khong biet MAX(cnt) that su bang 3 hay bang 3000. observed_expr giu
     * lai con so do; `expected` ghi nguong de nguoi doc khong phai mo rule set ra tra.
     */
    public String observedMetricName() {
        return metricName() + "__observed";
    }

    /**
     * Ten cot metric cua rule nay trong truy van aggregate - phai la dinh danh Spark hop le.
     * Khong can nhet stage vao ten: moi stage do bang mot truy van rieng nen pham vi ten la
     * trong mot stage.
     */
    public String metricName() {
        return "dq_" + ruleId;
    }

    /**
     * Ty le vi pham suy tu gia tri quan sat, hoac null khi khong suy duoc.
     *
     * Quy doi nhi phan (vi pham -> 100%) danh mat thong tin ma framework DA do duoc: 9 dong
     * trung khoa tren 480 dong va 400 dong trung khoa deu thanh 100%, va diem cua mot
     * checkpoint it rule bi mot rule muc tap lan at hoan toan (do that: 0.44 so voi 0.98 cho
     * CUNG mot loi, chi vi so rule sach dung canh khac nhau).
     */
    public Double violationPctFrom(double observedValue, long totalRows) {
        if (UNIT_ROWS.equals(observedUnit)) {
            if (totalRows <= 0) return null;
            return Math.min(100.0, Math.max(0.0, observedValue * 100.0 / totalRows));
        }
        if (UNIT_PERCENT.equals(observedUnit)) {
            return Math.min(100.0, Math.max(0.0, observedValue));
        }
        return null;
    }
}
