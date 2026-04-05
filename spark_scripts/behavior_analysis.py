from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Customer Behavior Analysis") \
    .getOrCreate()

df = spark.read.csv("hdfs:///user/aaqib/input_projects/5_customer_behavior/*.csv", header=True, inferSchema=True)

#df.show(5)
#df.printSchema()

df.groupBy("event_type").count().show()

df.filter(df.event_type == "view") \
  .groupBy("product_id") \
  .count() \
  .orderBy("count", ascending=False) \
  .show(10)


df.filter(df.event_type == "purchase") \
  .groupBy("product_id") \
  .count() \
  .orderBy("count", ascending=False) \
  .show(10)


df.filter(df.event_type == "purchase") \
  .groupBy("brand") \
  .count() \
  .orderBy("count", ascending=False) \
  .show(10)

df.filter(df.event_type == "purchase") \
  .groupBy("brand") \
  .sum("price") \
  .orderBy("sum(price)", ascending=False) \
  .show(10)

df.groupBy("user_id") \
  .count() \
  .orderBy("count", ascending=False) \
  .show(10)

df.filter(df.event_type == "cart") \
  .groupBy("product_id") \
  .count() \
  .orderBy("count", ascending=False) \
  .show(10)

df.filter(df.event_type == "purchase") \
  .groupBy("category_code") \
  .count() \
  .orderBy("count", ascending=False) \
  .show(10)

df.groupBy("category_code") \
  .avg("price") \
  .orderBy("avg(price)", ascending=False) \
  .show(10)

df.select("user_id").distinct().count()

df.groupBy("user_session") \
  .count() \
  .orderBy("count", ascending=False) \
  .show(10)

from pyspark.sql.functions import hour

df.groupBy(hour("event_time").alias("hour")) \
  .count() \
  .orderBy("hour") \
  .show(24)

df.filter(df.event_type == "purchase") \
  .orderBy("price", ascending=False) \
  .select("user_id","product_id","price") \
  .show(10)

# Identify users who added to cart but did not purchase 
cart_users = df.filter(df.event_type == "cart").select("user_id").distinct()
purchase_users = df.filter(df.event_type == "purchase").select("user_id").distinct()

cart_users.subtract(purchase_users).show(10)

from pyspark.sql.functions import sum

df.filter(df.event_type == "purchase") \
  .select(sum("price")) \
  .show()

df.filter(df.event_type == "purchase") \
  .groupBy("brand") \
  .sum("price") \
  .write \
  .mode("overwrite") \
  .csv("file:///home/aaqib/PROJECTS/5_Customer_Behavior_Analysis_Using_Spark/output/brand_revenue")

