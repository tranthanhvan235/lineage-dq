package org.lineagedq.spark.dq;

import org.apache.spark.sql.types.StructType;

/**
 * Mot bang Iceberg cua framework DQ. DDL duoc sinh tu StructType nen bang chi duoc mo ta mot
 * lan - truoc day moi bang co hai ban (chuoi DDL + StructType) va khong gi bat chung khop nhau.
 */
public record DqTable(String name, StructType schema) {

    public static DqTable of(String name, StructType schema) {
        return new DqTable(name, schema);
    }

    /**
     * @param catalog vd "dq_iceberg"
     * @param db      vd "warehouse_a"
     */
    public String fqn(String catalog, String db) {
        return catalog + "." + db + "." + name;
    }

    /** format-version 2: can cho UPDATE/DELETE muc dong (dq_rules, dq_lineage_state). */
    public String createDdl(String catalog, String db) {
        return "CREATE TABLE IF NOT EXISTS " + fqn(catalog, db) + " (" + schema.toDDL() + ")"
                + " USING iceberg TBLPROPERTIES ('format-version'='2')";
    }
}
