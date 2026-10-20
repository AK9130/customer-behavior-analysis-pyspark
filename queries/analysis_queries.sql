# Spark SQL queries
df.createOrReplaceTempView("ecommerce_behavior")

spark.sql("""
SELECT event_type, COUNT(*) AS total_events
FROM ecommerce_behavior
GROUP BY event_type
""").show()

spark.sql("""
SELECT product_id, COUNT(*) AS total_views
FROM ecommerce_behavior
WHERE event_type = 'view'
GROUP BY product_id
ORDER BY total_views DESC
LIMIT 10
""").show()

spark.sql("""
SELECT product_id, COUNT(*) AS total_purchases
FROM ecommerce_behavior
WHERE event_type = 'purchase'
GROUP BY product_id
ORDER BY total_purchases DESC
LIMIT 10
""").show()

spark.sql("""
SELECT brand, COUNT(*) AS total_purchases
FROM ecommerce_behavior
WHERE event_type = 'purchase' AND brand IS NOT NULL
GROUP BY brand
ORDER BY total_purchases DESC
LIMIT 10
""").show()

spark.sql("""
SELECT brand, CAST(SUM(price) AS DECIMAL(15,2)) AS total_revenue
FROM ecommerce_behavior
WHERE event_type = 'purchase'  AND brand IS NOT NULL
GROUP BY brand
ORDER BY total_revenue DESC
LIMIT 10
""").show()

