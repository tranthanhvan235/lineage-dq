package org.lineagedq.spark.dq;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Ghi vao bang Iceberg cua framework DQ: tao bang neu chua co, append, va NUOT MOI LOI.
 *
 * Nuot loi la bat bien cua ca framework - bang thong ke hong khong duoc lam hong pipeline.
 */
public final class IcebergSink {

    private static final Logger LOG = LoggerFactory.getLogger(IcebergSink.class);

    private final SparkSession spark;
    private final String catalog;
    private final String db;

    public IcebergSink(SparkSession spark, String catalog, String db) {
        this.spark = spark;
        this.catalog = catalog;
        this.db = db;
    }

    public String fqn(DqTable table) {
        return table.fqn(catalog, db);
    }

    /** @return so dong da ghi; 0 khi khong co gi de ghi hoac khi that bai. */
    public int append(DqTable table, List<Row> rows) {
        if (rows == null || rows.isEmpty()) return 0;
        try {
            spark.sql(table.createDdl(catalog, db));
            spark.createDataFrame(rows, table.schema()).writeTo(fqn(table)).append();
            LOG.info("=== IcebergSink: +{} dong -> {}", rows.size(), table.name());
            return rows.size();
        } catch (Exception e) {  // xem javadoc lop
            LOG.error("=== IcebergSink: ghi {} that bai (bo qua): {}", table.name(), e.toString());
            return 0;
        }
    }

    /**
     * Dem dong khop dieu kien, cho kiem tra idempotent truoc khi ghi.
     *
     * @return -1 khi khong doc duoc; caller nen van ghi (tha trung con hon mat).
     */
    public long count(DqTable table, String whereClause) {
        try {
            spark.sql(table.createDdl(catalog, db));
            return spark.sql("SELECT count(*) FROM " + fqn(table) + " WHERE " + whereClause)
                    .first().getLong(0);
        } catch (Exception e) {
            LOG.warn("=== IcebergSink: khong dem duoc {} ({}): {}", table.name(), whereClause, e.toString());
            return -1;
        }
    }

    /** Chay mot cau DDL/DML tren bang (UPDATE, DELETE...). @return true neu thanh cong. */
    public boolean exec(DqTable table, String sqlAfterTableName) {
        try {
            spark.sql(table.createDdl(catalog, db));
            spark.sql(sqlAfterTableName.replace("{t}", fqn(table)));
            return true;
        } catch (Exception e) {
            LOG.warn("=== IcebergSink: cau lenh tren {} that bai: {}", table.name(), e.toString());
            return false;
        }
    }

    /** Boc chuoi thanh literal SQL. */
    public static String lit(String s) {
        return s == null ? "NULL" : "'" + s.replace("'", "''") + "'";
    }
}
