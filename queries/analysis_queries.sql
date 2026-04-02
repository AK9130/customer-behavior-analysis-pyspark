-- Event type count
SELECT event_type, COUNT(*) 
FROM ecommerce_behavior
GROUP BY event_type;

-- Top viewed products
SELECT product_id, COUNT(*) 
FROM ecommerce_behavior
WHERE event_type='view'
GROUP BY product_id
ORDER BY COUNT(*) DESC
LIMIT 10;

-- Top purchased products
SELECT product_id, COUNT(*) 
FROM ecommerce_behavior
WHERE event_type='purchase'
GROUP BY product_id
ORDER BY COUNT(*) DESC
LIMIT 10;

-- Top brands by purchase
SELECT brand, COUNT(*) 
FROM ecommerce_behavior
WHERE event_type='purchase'
GROUP BY brand
ORDER BY COUNT(*) DESC
LIMIT 10;

-- Revenue by brand
SELECT brand, SUM(price) 
FROM ecommerce_behavior
WHERE event_type='purchase'
GROUP BY brand
ORDER BY SUM(price) DESC;
