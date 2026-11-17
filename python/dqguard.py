"""Python binding for DqGuard (the Java checkpoint library) over py4j.

    with DqGuardPy(spark, "warehouse/shop.db/customer_monthly") as dq:
        dq.check("src_orders", orders)
        ...
        dq.before_write()
        final.write.parquet(out)
        dq.after_write()

dataset_name must end with the path the job writes to: DqGuard matches it against the
OpenLineage output dataset to find the job's column lineage. dq_key (the folder under /dq/)
is the last path segment of dataset_name.

The DQ store lives under DQ_STORE_LOCAL (local file system) when set, otherwise under
`base` (any Hadoop URI, e.g. hdfs://namenode:9000).
"""
import os


class DqGuardPy:
    def __init__(self, spark, dataset_name, dq_key=None, base=None, app_id=None):
        derived = dataset_name.rstrip("/").split("/")[-1]
        if dq_key and dq_key != derived:
            raise ValueError(f"dq_key '{dq_key}' must be the last segment of dataset_name ('{derived}')")
        local = os.getenv("DQ_STORE_LOCAL")
        base = base or (f"file://{os.path.abspath(local)}" if local else None)
        if not base:
            raise ValueError("set DQ_STORE_LOCAL or pass base=<hadoop uri>")
        jvm = spark.sparkContext._jvm
        builder = (jvm.org.lineagedq.spark.dq.DqGuard.builder()
                   .spark(spark._jsparkSession)
                   .appId(app_id or spark.sparkContext.applicationId)
                   .dqKey(derived)
                   .datasetName(dataset_name)
                   .hadoop(spark.sparkContext._jsc.hadoopConfiguration(), base))
        self._g = builder.build()

    def check(self, stage, df):
        self._g.check(stage, df._jdf)

    def before_write(self):
        self._g.beforeWrite()

    def after_write(self):
        self._g.afterWrite()

    def close(self):
        self._g.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()
        return False
