"""Example pipeline: monthly revenue per customer, with four DQ checkpoints.

    src_orders, src_customers -> jn_customer (LEFT JOIN) -> tf_derive (net_amount, order_month)
                              -> tf_customer_month (aggregate) -> write parquet

Run through examples/orders/run.sh, which sets the DQ store, the agent and OpenLineage.
"""
import argparse
import sys
from pathlib import Path

from pyspark.sql import SparkSession, functions as F

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "python"))
from dqguard import DqGuardPy  # noqa: E402

DATASET = "warehouse/shop.db/customer_monthly"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", required=True, help="Folder written by gen_data.py")
    ap.add_argument("--out", required=True, help="Output root; the table goes to <out>/" + DATASET)
    a = ap.parse_args()
    data = Path(a.data).resolve()
    out = f"file://{Path(a.out).resolve()}/{DATASET}"

    spark = SparkSession.builder.appName("orders-example").getOrCreate()
    with DqGuardPy(spark, DATASET) as dq:
        orders = spark.read.parquet(f"file://{data}/orders")
        dq.check("src_orders", orders)
        customers = spark.read.parquet(f"file://{data}/customers")
        dq.check("src_customers", customers)

        jn = orders.join(customers, "customer_id", "left")
        dq.check("jn_customer", jn)

        derive = (jn.filter(F.col("status") == "completed")
                    .withColumn("gross_amount", F.col("quantity") * F.col("unit_price"))
                    .withColumn("net_amount", F.round(F.col("gross_amount") * (1 - F.col("discount")), 2))
                    .withColumn("order_month", F.substring("order_date", 1, 7)))
        dq.check("tf_derive", derive)

        final = (derive.groupBy("customer_id", "customer_code", "segment", "order_month")
                 .agg(F.count("*").alias("order_count"),
                      F.sum("quantity").alias("total_quantity"),
                      F.round(F.sum("net_amount"), 2).alias("net_revenue")))
        dq.check("tf_customer_month", final)

        dq.before_write()
        final.write.mode("overwrite").parquet(out)
        dq.after_write()
    print(f"[orders] -> {out}")
    spark.stop()


if __name__ == "__main__":
    main()
