from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark = SparkSession.builder.getOrCreate()
 
 
# 1. Prepare Orders Data
def prepare_orders(fact_orders, fact_order_items):
    """
    Joins orders and items, extracting the flags needed for operational metrics.
    """
    return fact_orders.alias("o").join(
        fact_order_items.alias("oi"),
        "order_id",
        "inner"
    ).select(
        "oi.seller_key",
        "o.order_id",
        F.to_date("o.order_purchase_ts").alias("order_date"),
        "o.delivered_to_carrier_flag",
        "o.late_to_carrier_flag",
        "o.order_status"
    ).dropDuplicates()
 
 
# 2. Prepare Reviews
def prepare_pit_reviews(fact_reviews, fact_orders):
    """
    Creates a historical ledger of reviews to prevent data leakage.
    Each review state gets a 'valid_from' and 'valid_to' date.
 
    """

    # reviews attached to CARRIER-caused late deliveries are removed first, so the seller's review metric only reflects outcomes the seller could influence.
    order_attrib = fact_orders.select(
        "order_id",
        F.when(
            (F.col("late_to_customer_flag") == 1) & (F.col("late_to_carrier_flag") == 0),
            1
        ).otherwise(0).alias("carrier_caused_late")
    ) 
 
    reviews_clean = fact_reviews.join(order_attrib, "order_id", "left") \
        .filter(F.coalesce(F.col("carrier_caused_late"), F.lit(0)) == 0) \
        .drop("carrier_caused_late")
 
    first_dates = reviews_clean.groupBy("seller_key", "order_id").agg(
        F.min(F.to_date("review_creation_date")).alias("first_review_date")
    )
 
    # review_version_number orders the versions created on the same day
    w_history = Window.partitionBy("seller_key", "order_id").orderBy("review_creation_date", "review_version_number")
 
    pit_reviews = reviews_clean.join(first_dates, ["seller_key", "order_id"], "inner") \
        .withColumn("valid_from", F.to_date("review_creation_date")) \
        .withColumn("valid_to", F.coalesce(
            F.date_sub(F.to_date(F.lead("review_creation_date").over(w_history)), 1),
            F.to_date(F.lit("2099-12-31"))  # Currently active state
        )) \
        .withColumn("is_claim", F.when(F.col("review_score") < 3, 1).otherwise(0))
 
    return pit_reviews
 
 
# 3. Create Dense Calendar with Lookback Windows
def create_dense_calendar(df_orders, df_items, window_days, lag_days):
    """
    Generates the base matrix of Seller x Date, including
    lookback window boundaries for each reference date.
    """
    sellers = df_items.select("seller_key").distinct()
 
    date_bounds = df_orders.select(
        F.min("order_date").alias("min_date"),
        F.max("order_date").alias("max_date")
    ).collect()[0]
 
    calendar = spark.sql(f"""
        SELECT EXPLODE(SEQUENCE(
            DATE '{date_bounds["min_date"]}',
            DATE '{date_bounds["max_date"]}',
            INTERVAL 1 DAY
        )) AS reference_date
    """)
 
    # Cross join and define the window (window_days days, ending lag_days before the reference date)
    return sellers.crossJoin(calendar) \
        .withColumn("window_start", F.date_sub("reference_date", window_days + lag_days - 1)) \
        .withColumn("window_end", F.date_sub("reference_date", lag_days))
 
 
# 4. Point-in-Time Aggregation  
def calculate_pit_metrics(calendar, pit_orders, pit_reviews, window_days):
    """
    Joins events strictly within the window.
    """
    suffix = f"_{window_days}d"
 
    # 4.1 Calculate Order Metrics
    order_metrics = calendar.alias("c").join(
        pit_orders.alias("o"),
        (F.col("o.seller_key") == F.col("c.seller_key")) &
        (F.col("o.order_date").between(F.col("c.window_start"), F.col("c.window_end"))),
        "left"
    ).groupBy("c.seller_key", "c.reference_date").agg(
        F.count("o.order_id").alias("orders" + suffix),
        F.sum(F.when(F.col("o.delivered_to_carrier_flag") == 1, 1).otherwise(0)).alias("delivered_to_carrier" + suffix),
        F.sum(F.when(F.col("o.late_to_carrier_flag") == 1, 1).otherwise(0)).alias("late_to_carrier" + suffix)
        # cancellations_60d removed: cancellations by seller are registered as order_status 'unavailable' 
        # orders with that status lack item/seller records, so this metric was unmeasurable at seller level.
    )
 
    # 4.2 Calculate Review Metrics
    review_metrics = calendar.alias("c").join(
        pit_reviews.alias("r"),
        (F.col("r.seller_key") == F.col("c.seller_key")) &
        (F.col("r.first_review_date").between(F.col("c.window_start"), F.col("c.window_end"))) &
        (F.col("c.reference_date").between(F.col("r.valid_from"), F.col("r.valid_to"))),
        "left"
    ).groupBy("c.seller_key", "c.reference_date").agg(
        F.count("r.order_id").alias("reviews" + suffix),
        F.sum("r.is_claim").alias("claims" + suffix),
        F.sum("r.review_score").alias("sum_score" + suffix)
    )
 
    # 4.3 Combine and calculate final rates
    combined = order_metrics.join(review_metrics, ["seller_key", "reference_date"], "inner")
 
    # Avoiding inconsistencies when dividing by 0
    def safe_div(numerator, denominator):
        return F.col(numerator) / F.when(F.col(denominator) == 0, F.lit(None)).otherwise(F.col(denominator))
 
    df_final = combined.withColumn("avg_review_score" + suffix, F.round(safe_div("sum_score" + suffix, "reviews" + suffix), 2)) \
                       .withColumn("late_rate", F.round(safe_div("late_to_carrier" + suffix, "delivered_to_carrier" + suffix), 4)) \
                       .withColumn("claim_rate", F.round(safe_div("claims" + suffix, "reviews" + suffix), 4)) \
                       .withColumn("reliability_orders", F.round(F.col("orders" + suffix) / (F.col("orders" + suffix) + 20), 2)) \
                       .withColumn("reliability_reviews", F.round(F.col("reviews" + suffix) / (F.col("reviews" + suffix) + 20), 2)) \
                       .drop("sum_score" + suffix) \
                       .sort(F.asc("seller_key"), F.asc("reference_date"))
    # cancel_rate removed
 
    # Keep only active sellers based on the rules
    return df_final.filter((F.col("orders" + suffix) > 0) | (F.col("reviews" + suffix) > 0))


# 5. Daily activity per seller (used in Seller Index to compare windows)
def create_daily_activity(calendar, pit_orders, pit_reviews):
    """
    Orders (by purchase date) and reviewed orders (by first review date) per seller and day.
    """
    daily_orders = pit_orders.groupBy("seller_key", "order_date").agg(F.count("order_id").alias("orders")) \
        .withColumnRenamed("order_date", "reference_date")

    daily_reviews = pit_reviews.select("seller_key", "order_id", "first_review_date").distinct() \
        .groupBy("seller_key", "first_review_date").agg(F.count("order_id").alias("reviews_day")) \
        .withColumnRenamed("first_review_date", "reference_date")

    return calendar.select("seller_key", "reference_date") \
        .join(daily_orders, ["seller_key", "reference_date"], "left") \
        .join(daily_reviews, ["seller_key", "reference_date"], "left") \
        .fillna(0, subset=["orders", "reviews_day"])
