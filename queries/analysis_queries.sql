# Spark SQL queries
df.createOrReplaceTempView("ecommerce_behavior")

# 1. Event Type-wise Total Events
spark.sql("""
SELECT event_type, COUNT(*) AS total_events
FROM ecommerce_behavior
GROUP BY event_type
""").show()

# 2. Top 10 Most Viewed Products
spark.sql("""
SELECT product_id, COUNT(*) AS total_views
FROM ecommerce_behavior
WHERE event_type = 'view'
GROUP BY product_id
ORDER BY total_views DESC
LIMIT 10
""").show()

# 3. Top 10 Most Purchased Products
spark.sql("""
SELECT product_id, COUNT(*) AS total_purchases
FROM ecommerce_behavior
WHERE event_type = 'purchase'
GROUP BY product_id
ORDER BY total_purchases DESC
LIMIT 10
""").show()

# 4. Top 10 Brands by Purchase Count
spark.sql("""
SELECT brand, COUNT(*) AS total_purchases
FROM ecommerce_behavior
WHERE event_type = 'purchase' AND brand IS NOT NULL
GROUP BY brand
ORDER BY total_purchases DESC
LIMIT 10
""").show()

# 5. Top 10 Brands by Total Revenue
spark.sql("""
SELECT brand, CAST(SUM(price) AS DECIMAL(15,2)) AS total_revenue
FROM ecommerce_behavior
WHERE event_type = 'purchase' AND brand IS NOT NULL
GROUP BY brand
ORDER BY total_revenue DESC
LIMIT 10
""").show()
