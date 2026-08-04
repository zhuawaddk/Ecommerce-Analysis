"""
巴西电商订单数据多维度分析
技术栈: PySpark (Spark SQL / DataFrame API)
数据集: Brazilian E-Commerce Public Dataset (Kaggle)
"""
import os
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import FloatType

DATA_DIR = os.path.join(os.path.dirname(__file__), "data")
OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "output")

spark = SparkSession.builder \
    .appName("BrazilianEcommerceAnalysis") \
    .master("local[*]") \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ============================================================
# 1. 数据加载
# ============================================================
print("=" * 60)
print("[1/6] 加载数据...")

df_orders = spark.read.option("header", True).csv(f"{DATA_DIR}/olist_orders_dataset.csv")
df_items = spark.read.option("header", True).csv(f"{DATA_DIR}/olist_order_items_dataset.csv")
df_products = spark.read.option("header", True).csv(f"{DATA_DIR}/olist_products_dataset.csv")
df_customers = spark.read.option("header", True).csv(f"{DATA_DIR}/olist_customers_dataset.csv")
df_payments = spark.read.option("header", True).csv(f"{DATA_DIR}/olist_order_payments_dataset.csv")
df_category = spark.read.option("header", True).csv(f"{DATA_DIR}/product_category_name_translation.csv")

print(f"  订单表: {df_orders.count():,} 行")
print(f"  订单商品表: {df_items.count():,} 行")
print(f"  商品表: {df_products.count():,} 行")
print(f"  客户表: {df_customers.count():,} 行")
print(f"  支付表: {df_payments.count():,} 行")

# ============================================================
# 2. 数据清洗
# ============================================================
print("\n" + "=" * 60)
print("[2/6] 数据清洗...")

df_items = df_items.withColumn("price", F.col("price").cast(FloatType())) \
    .withColumn("freight_value", F.col("freight_value").cast(FloatType()))

df_orders = df_orders.withColumn("order_purchase_timestamp",
    F.to_timestamp(F.col("order_purchase_timestamp"), "yyyy-MM-dd HH:mm:ss"))

df_orders = df_orders.filter(
    F.col("order_status").isin("delivered", "shipped", "invoiced")
)

df_payments = df_payments.withColumn("payment_value", F.col("payment_value").cast(FloatType()))

payment_total = df_payments.groupBy("order_id").agg(
    F.sum("payment_value").alias("total_payment")
)

# 异常金额过滤 (保留 1~10000 之间的订单)
payment_total = payment_total.filter(
    (F.col("total_payment") > 1) & (F.col("total_payment") < 10000)
)

print(f"  清洗后有效订单: {df_orders.count():,} 行")

# ============================================================
# 3. 构建交易宽表 (多表 JOIN)
# ============================================================
print("\n" + "=" * 60)
print("[3/6] 构建交易宽表...")

df_wide = df_orders \
    .join(df_items, "order_id", "inner") \
    .join(df_products, "product_id", "left") \
    .join(df_customers, "customer_id", "left") \
    .join(payment_total, "order_id", "inner") \
    .select(
        df_orders.order_id,
        df_orders.customer_id,
        df_orders.order_purchase_timestamp,
        df_orders.order_status,
        df_customers.customer_city,
        df_customers.customer_state,
        df_products.product_category_name,
        df_items.price,
        df_items.freight_value,
        F.col("total_payment")
    ) \
    .withColumn("order_month", F.date_format("order_purchase_timestamp", "yyyy-MM")) \
    .withColumn("order_year_month", F.date_format("order_purchase_timestamp", "yyyyMM").cast("int"))

df_wide = df_wide.join(
    df_category.withColumnRenamed("product_category_name", "cat_pt"),
    df_wide.product_category_name == F.col("cat_pt"),
    "left"
).drop("cat_pt")

df_wide.cache()
print(f"  宽表行数: {df_wide.count():,}")

# ============================================================
# 4. 多维度分析
# ============================================================
os.makedirs(OUTPUT_DIR, exist_ok=True)


def save_csv(df, filename, limit=None):
    """将 Spark DataFrame 转为 Pandas 写入 CSV"""
    path = os.path.join(OUTPUT_DIR, filename)
    pdf = df.limit(limit).toPandas() if limit else df.toPandas()
    pdf.to_csv(path, index=False, encoding="utf-8-sig")
    print(f"  -> 已保存: {path} ({len(pdf)} 行)")

# --- 4.1 各州销售额排名 ---
print("\n" + "=" * 60)
print("[4/6] 各州销售额分析...")

state_analysis = df_wide.groupBy("customer_state").agg(
    F.round(F.sum("total_payment"), 2).alias("total_gmv"),
    F.countDistinct("order_id").alias("order_count"),
    F.round(F.avg("total_payment"), 2).alias("avg_order_value")
).orderBy(F.col("total_gmv").desc())

state_analysis.show(20, truncate=False)
save_csv(state_analysis, "state_analysis.csv")

# --- 4.2 月度销售趋势 + 环比增长率 ---
print("\n" + "=" * 60)
print("[5/6] 月度销售趋势分析...")

monthly_sales = df_wide.groupBy("order_month").agg(
    F.round(F.sum("total_payment"), 2).alias("monthly_gmv"),
    F.countDistinct("order_id").alias("order_count"),
    F.round(F.avg("total_payment"), 2).alias("avg_order_value")
).orderBy("order_month")

window_spec = Window.orderBy("order_month")
monthly_sales = monthly_sales.withColumn(
    "prev_month_gmv", F.lag("monthly_gmv").over(window_spec)
).withColumn(
    "mom_growth_pct",
    F.round((F.col("monthly_gmv") - F.col("prev_month_gmv")) / F.col("prev_month_gmv") * 100, 2)
)

monthly_sales.show(30, truncate=False)
save_csv(monthly_sales, "monthly_sales.csv")

# --- 4.3 品类销售排名 (Window 函数) ---
print("\n" + "=" * 60)
print("[6/6] 品类销售排名 + RFM 客户分层...")

category_rank = df_wide.groupBy("product_category_name_english").agg(
    F.round(F.sum("total_payment"), 2).alias("total_gmv"),
    F.countDistinct("order_id").alias("order_count"),
    F.round(F.avg("total_payment"), 2).alias("avg_order_value")
).withColumn(
    "gmv_rank", F.row_number().over(Window.orderBy(F.col("total_gmv").desc()))
).orderBy("gmv_rank")

category_rank.show(15, truncate=False)
save_csv(category_rank, "category_ranking.csv")

# --- 4.4 RFM 客户分层 ---
print("\n" + "=" * 60)
print("RFM 客户分层分析...")

ref_date = df_wide.select(F.max("order_purchase_timestamp")).collect()[0][0]

rfm = df_wide.groupBy("customer_id").agg(
    F.datediff(F.lit(ref_date), F.max("order_purchase_timestamp")).alias("recency"),
    F.countDistinct("order_id").alias("frequency"),
    F.round(F.sum("total_payment"), 2).alias("monetary")
)

rfm = rfm.withColumn("r_score", F.ntile(4).over(Window.orderBy(F.col("recency").desc()))) \
    .withColumn("f_score", F.ntile(4).over(Window.orderBy("frequency"))) \
    .withColumn("m_score", F.ntile(4).over(Window.orderBy("monetary")))

rfm = rfm.withColumn("rfm_score", F.col("r_score") + F.col("f_score") + F.col("m_score"))

rfm = rfm.withColumn("customer_segment",
    F.when(F.col("rfm_score") >= 10, "高价值客户(前25%)")
     .when(F.col("rfm_score") >= 7, "重要客户(25-50%)")
     .when(F.col("rfm_score") >= 5, "一般客户(50-75%)")
     .otherwise("低价值客户(后25%)")
)

rfm_segment = rfm.groupBy("customer_segment").agg(
    F.count("customer_id").alias("customer_count"),
    F.round(F.avg("monetary"), 2).alias("avg_purchase_amount"),
    F.round(F.avg("frequency"), 2).alias("avg_order_count")
).orderBy(F.col("customer_segment").asc())

rfm_segment.show(truncate=False)
save_csv(rfm_segment, "rfm_segmentation.csv")

df_wide.unpersist()
spark.stop()

print("\n" + "=" * 60)
print("分析完成! 结果已保存至 output/ 目录")
print(f"  - state_analysis.csv         各州销售额排名")
print(f"  - monthly_sales.csv           月度销售趋势(含环比增长率)")
print(f"  - category_ranking.csv        品类销售排名")
print(f"  - rfm_segmentation.csv        客户RFM分层")
