from pyspark.sql import SparkSession

# 1. Create Spark Session
spark = SparkSession.builder \
    .appName("Customer Behavior Analysis") \
    .getOrCreate()

# 2. Load Data from HDFS
df = spark.read.csv(
    "hdfs:///user/aaqib/input_projects/5_customer_behavior/*.csv",
    header=True,
    inferSchema=True
)

# 3. Event Type Distribution
df.groupBy("event_type").count().show()

# 4. Top Viewed Products
df.filter(df.event_type == "view") \
  .groupBy("product_id") \
  .count() \
  .orderBy("count", ascending=False) \
  .show(10)

# 5. Top Purchased Products
df.filter(df.event_type == "purchase") \
  .groupBy("product_id") \
  .count() \
  .orderBy("count", ascending=False) \
  .show(10)

# 6. Top Brands by Purchases
df.filter(df.event_type == "purchase") \
  .groupBy("brand") \
  .count() \
  .orderBy("count", ascending=False) \
  .show(10)

# 7. Revenue by Brand
df.filter(df.event_type == "purchase") \
  .groupBy("brand") \
  .sum("price") \
  .orderBy("sum(price)", ascending=False) \
  .show(10)

# 8. Most Active Users
df.groupBy("user_id") \
  .count() \
  .orderBy("count", ascending=False) \
  .show(10)

# 9. Most Added to Cart Products
df.filter(df.event_type == "cart") \
  .groupBy("product_id") \
  .count() \
  .orderBy("count", ascending=False) \
  .show(10)

# 10. Top Categories by Purchase
df.filter(df.event_type == "purchase") \
  .groupBy("category_code") \
  .count() \
  .orderBy("count", ascending=False) \
  .show(10)

# 11. Average Price by Category
df.groupBy("category_code") \
  .avg("price") \
  .orderBy("avg(price)", ascending=False) \
  .show(10)

# 12. Total Unique Users
df.select("user_id").distinct().count()

# 13. Most Active Sessions
df.groupBy("user_session") \
  .count() \
  .orderBy("count", ascending=False) \
  .show(10)


# 14. Hourly Activity Analysis
from pyspark.sql.functions import hour

df.groupBy(hour("event_time").alias("hour")) \
  .count() \
  .orderBy("hour") \
  .show(24)

# 15. Highest Value Purchases
df.filter(df.event_type == "purchase") \
  .orderBy("price", ascending=False) \
  .select("user_id", "product_id", "price") \
  .show(10)


# 16. Cart Abandonment Analysis

# Identify users who added to cart but did not purchase
cart_users = df.filter(df.event_type == "cart") \
  .select("user_id").distinct()

purchase_users = df.filter(df.event_type == "purchase") \
  .select("user_id").distinct()

cart_users.subtract(purchase_users).show(10)

# 17. Total Revenue
from pyspark.sql.functions import sum

df.filter(df.event_type == "purchase") \
  .select(sum("price")) \
  .show()

# 18. Save Revenue by Brand to HDFS
df.filter(df.event_type == "purchase") \
  .groupBy("brand") \
  .sum("price") \
  .write \
  .mode("overwrite") \
  .csv("hdfs:///user/aaqib/output_projects/5_customer_behavior/brand_revenue")
