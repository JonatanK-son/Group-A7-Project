"""PySpark transformations including a window-function analysis."""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from src.logger import StructuredLogger

log = StructuredLogger("spark_transforms")


# ── Session factory ───────────────────────────────────────────────────────────

def get_spark_session(app_name: str = "EcomPipeline-A7") -> SparkSession:
    spark = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.shuffle.partitions", "50")
        .config("spark.driver.memory", "4g")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.ui.enabled", "true")
        .config("spark.ui.port", "4040")
        .getOrCreate()
    )
    return spark


def get_spark_dashboard_url(spark: SparkSession) -> str:
    """Return the Spark web UI URL (e.g. http://localhost:4040)."""
    sc = spark.sparkContext
    ui_url = sc.uiWebUrl  # None if the UI is disabled
    if ui_url:
        return ui_url
    port = sc._conf.get("spark.ui.port", "4040")
    return f"http://localhost:{port}"


# ── Loader ────────────────────────────────────────────────────────────────────

def load_csv_spark(spark: SparkSession, path: str | list[str]) -> DataFrame:
    """
    Load one or more CSV files into a Spark DataFrame.
    *path* can be a single path string or a list of path strings.
    Forward slashes are required on Windows; convert before calling.
    """
    paths = path if isinstance(path, list) else [path]
    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .option("timestampFormat", "yyyy-MM-dd HH:mm:ss z")
        .option("mode", "DROPMALFORMED")
        .csv(paths)
    )
    log.info("spark_csv_loaded", files=len(paths), partitions=df.rdd.getNumPartitions())
    return df


# ── Helper ────────────────────────────────────────────────────────────────────

def _with_top_category(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "top_category",
        F.split(F.coalesce(F.col("category_code"), F.lit("unknown")), r"\.")[0],
    )


# ── Analysis 1: Revenue by category (filter + aggregate) ─────────────────────

def compute_revenue_by_category_spark(df: DataFrame) -> DataFrame:
    result = (
        _with_top_category(df.filter(F.col("event_type") == "purchase"))
        .groupBy("top_category")
        .agg(
            F.sum("price").alias("total_revenue"),
            F.count("*").alias("num_purchases"),
            F.avg("price").alias("avg_price"),
        )
        .orderBy(F.desc("total_revenue"))
    )
    log.info("spark_revenue_done")
    return result


# ── Analysis 2: Conversion funnel (pivot aggregate) ───────────────────────────

def compute_conversion_funnel_spark(df: DataFrame) -> DataFrame:
    with_cat = _with_top_category(df)
    funnel = (
        with_cat.groupBy("top_category", "event_type")
        .count()
        .groupBy("top_category")
        .pivot("event_type")
        .sum("count")
        .fillna(0)
    )
    funnel = funnel.withColumn(
        "purchase_rate",
        F.when(F.col("view") > 0, F.col("purchase") / F.col("view")).otherwise(None),
    ).withColumn(
        "cart_rate",
        F.when(F.col("view") > 0, F.col("cart") / F.col("view")).otherwise(None),
    )
    log.info("spark_funnel_done")
    return funnel


# ── Analysis 3: Window function — rank products within category ───────────────

def compute_window_rank_spark(df: DataFrame) -> DataFrame:
    """Top-5 products by revenue within each category using a window function."""
    product_revenue = (
        _with_top_category(df.filter(F.col("event_type") == "purchase"))
        .groupBy("top_category", "product_id")
        .agg(
            F.sum("price").alias("product_revenue"),
            F.count("*").alias("num_sales"),
        )
    )
    win = Window.partitionBy("top_category").orderBy(F.desc("product_revenue"))
    ranked = (
        product_revenue
        .withColumn("rank_in_category", F.rank().over(win))
        .filter(F.col("rank_in_category") <= 5)
        .orderBy("top_category", "rank_in_category")
    )
    log.info("spark_window_rank_done")
    return ranked


# ── Analysis 4: Hourly activity (assign + group) ──────────────────────────────

def compute_hourly_activity_spark(df: DataFrame) -> DataFrame:
    result = (
        df.withColumn("hour", F.hour(F.col("event_time")))
        .groupBy("hour", "event_type")
        .count()
        .orderBy("hour", "event_type")
    )
    log.info("spark_hourly_done")
    return result
