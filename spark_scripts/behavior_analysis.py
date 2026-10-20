from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# 1. Create Spark Session
spark = SparkSession.builder \
    .appName("Customer Behavior Analysis") \
    .getOrCreate()

input_path = "hdfs://localhost:9000/user/aaqib/input_projects/5_customer_behavior/2019-Oct.csv"
output_path = "hdfs://localhost:9000/user/aaqib/output_projects/5_customer_behavior/brand_revenue_parquet"

# 2. Load Data from HDFS
df = spark.read.csv(input_path, header=True, inferSchema=True)

# Cache the DataFrame to avoid re-reading from HDFS for every analysis
df.cache()
print("Total rows:", df.count())

# 3. Data Quality Check: count null values in key columns
print("Null brand rows:", df.filter(df.brand.isNull()).count())
print("Null category_code rows:", df.filter(df.category_code.isNull()).count())
print("Null user_session rows:", df.filter(df.user_session.isNull()).count())

# 4. Event Type Distribution
df.groupBy("event_type").count().show()

# 5. Top Viewed Products
df.filter(df.event_type == "view") \
  .groupBy("product_id") \
  .count() \
  .orderBy("count", ascending=False) \
  .show(10)

# 6. Top Purchased Products
df.filter(df.event_type == "purchase") \
  .groupBy("product_id") \
  .count() \
  .orderBy("count", ascending=False) \
  .show(10)

# 7. Top Brands by Purchases (rows with null brand excluded)
df.filter(df.event_type == "purchase") \
  .filter(F.col("brand").isNotNull()) \
  .groupBy("brand") \
  .count() \
  .orderBy("count", ascending=False) \
  .show(10)

# 8. Revenue by Brand (rows with null brand excluded)
df.filter(df.event_type == "purchase") \
  .filter(F.col("brand").isNotNull()) \
  .groupBy("brand") \
  .sum("price") \
  .orderBy("sum(price)", ascending=False) \
  .show(10)

# 9. Most Active Users
df.groupBy("user_id") \
  .count() \
  .orderBy("count", ascending=False) \
  .show(10)

# 10. Most Added to Cart Products
df.filter(df.event_type == "cart") \
  .groupBy("product_id") \
  .count() \
  .orderBy("count", ascending=False) \
  .show(10)

# 11. Top Categories by Purchases (rows with null category excluded)
df.filter(df.event_type == "purchase") \
  .filter(F.col("category_code").isNotNull()) \
  .groupBy("category_code") \
  .count() \
  .orderBy("count", ascending=False) \
  .show(10)

# 12. Average Price by Category (rows with null category excluded)
df.filter(F.col("category_code").isNotNull()) \
  .groupBy("category_code") \
  .avg("price") \
  .orderBy("avg(price)", ascending=False) \
  .show(10)

# 13. Total Unique Users
print("Total unique users:", df.select("user_id").distinct().count())

# 14. Most Active Sessions
df.filter(F.col("user_session").isNotNull()) \
  .groupBy("user_session") \
  .count() \
  .orderBy("count", ascending=False) \
  .show(10)

# 15. Hourly Activity Analysis
df.groupBy(F.hour("event_time").alias("hour")) \
  .count() \
  .orderBy("hour") \
  .show(24)

# 16. Highest Value Purchases
df.select("user_id", "product_id", "price") \
  .filter(df.event_type == "purchase") \
  .orderBy("price", ascending=False) \
  .show(10)

# 17. Cart Abandonment Analysis
# Users who added to cart but never purchased
cart_users = df.filter(df.event_type == "cart") \
  .select("user_id").distinct()

purchase_users = df.filter(df.event_type == "purchase") \
  .select("user_id").distinct()

cart_users.subtract(purchase_users).show(10)

# 18. Total Revenue
df.filter(df.event_type == "purchase") \
  .select(F.sum("price").alias("total_revenue")) \
  .show()

# 19. Save Revenue by Brand to HDFS (Parquet format)
df.filter(df.event_type == "purchase") \
  .filter(F.col("brand").isNotNull()) \
  .groupBy("brand") \
  .sum("price") \
  .write \
  .mode("overwrite") \
  .parquet(output_path)

spark.stop()

# Run from the project root directory:
# spark-submit --master local[2] --driver-memory 2g spark_scripts/behavior_analysis.py
