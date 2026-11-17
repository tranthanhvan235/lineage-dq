"""Synthetic input for the orders example: customers and orders as parquet.

    python examples/orders/gen_data.py --out data/orders [--orders 50000]

Deterministic for a given --seed. Contains a few natural defects on purpose (orders with an
unknown customer, negative discounts) so that generated rules have something to find.
"""
import argparse
import random
from pathlib import Path

from pyspark.sql import SparkSession

SEGMENTS = ["consumer", "corporate", "small_business"]
REGIONS = ["north", "south", "east", "west"]
STATUSES = ["completed", "completed", "completed", "cancelled", "returned"]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True)
    ap.add_argument("--customers", type=int, default=2000)
    ap.add_argument("--orders", type=int, default=50000)
    ap.add_argument("--seed", type=int, default=7)
    a = ap.parse_args()
    rnd = random.Random(a.seed)

    customers = [(i, f"C{i:05d}", rnd.choice(SEGMENTS), rnd.choice(REGIONS))
                 for i in range(1, a.customers + 1)]
    orders = []
    for i in range(1, a.orders + 1):
        cust = rnd.randint(1, a.customers) if rnd.random() > 0.002 else a.customers + rnd.randint(1, 50)
        qty = rnd.randint(1, 10)
        price = round(rnd.uniform(2, 500), 2)
        disc = float(rnd.choice([0, 0, 0.05, 0.1, 0.2]) if rnd.random() > 0.001 else -0.1)
        orders.append((i, cust, f"2026-{rnd.randint(1, 6):02d}-{rnd.randint(1, 28):02d}",
                       rnd.choice(STATUSES), qty, price, disc))

    spark = SparkSession.builder.appName("orders-gen").getOrCreate()
    out = Path(a.out).resolve()
    spark.createDataFrame(customers, "customer_id long, customer_code string, segment string, region string") \
        .write.mode("overwrite").parquet(f"file://{out}/customers")
    spark.createDataFrame(orders, "order_id long, customer_id long, order_date string, status string, "
                                  "quantity int, unit_price double, discount double") \
        .write.mode("overwrite").parquet(f"file://{out}/orders")
    print(f"[gen] {len(customers)} customers, {len(orders)} orders -> {out}")
    spark.stop()


if __name__ == "__main__":
    main()
