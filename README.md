# Customer Behavior Analysis Using Apache Spark

## 1. Project Overview
This project analyzes e-commerce customer behavior data using **Apache Spark (PySpark)**. The data is stored in **Hadoop HDFS** and processed using Spark DataFrames and Spark SQL.
It examines customer events such as product views, cart additions, and purchases, and covers products, brands, categories, users, sessions, and revenue. Brand-wise purchase revenue is saved back to HDFS in **Parquet** format.

## 2. Problem Statement
E-commerce platforms generate large volumes of customer interaction events. This project uses Spark to summarize those events and identify patterns such as popular products, frequently purchased brands, customer activity over the day, and purchase revenue.

## 3. Dataset
The project uses e-commerce behavior data for **October 2019** (`2019-Oct.csv`, roughly 5 GB).

```text
data_sets/ecommerce_behavior/
└── 2019-Oct.csv
```

HDFS input path:

```text
/user/aaqib/input_projects/5_customer_behavior/
```

| Field | Description |
|---|---|
| `event_time` | Timestamp of the customer event (UTC) |
| `event_type` | Event type: `view`, `cart`, or `purchase` |
| `product_id` | Product identifier |
| `category_id` | Category identifier |
| `category_code` | Product category |
| `brand` | Product brand |
| `price` | Product price |
| `user_id` | Customer identifier |
| `user_session` | Session identifier |

## 4. Technologies Used

- Python
- Apache Spark (PySpark DataFrames)
- Spark SQL
- Hadoop HDFS
- Linux (Ubuntu)
- CSV input, Parquet output

## 5. Architecture

```text
CSV file
   |
   v
HDFS
   |
   v
PySpark application
   |
   v
Spark DataFrame
   |
   +---------------------------+
   |                           |
   v                           v
DataFrame analysis       Temporary SQL view
   |                           |
   +-------------+-------------+
                 |
                 v
          Analysis results
                 |
                 v
      Brand revenue (Parquet)
                 |
                 v
               HDFS
```

## 6. Analysis Performed

1. Count of events by event type
2. Top 10 viewed products
3. Top 10 purchased products
4. Top 10 brands by purchase count
5. Top 10 brands by revenue
6. Most active users
7. Most added-to-cart products
8. Top categories by purchase count
9. Average price by category
10. Total number of unique users
11. Most active sessions
12. Hourly activity
13. Highest-value purchases
14. Users who added to cart but have no purchase event in the loaded data
15. Total purchase revenue

## 7. Key Insights

### Dataset Scale
- Total events analyzed: **[X million]** rows (Oct + Nov 2019)
- Distinct users: **[X]**
- Distinct sessions: **[X]**

### Event Funnel
| Event Type | Count | % of Total |
|------------|-------|------------|
| view       | [X]   | [X%]       |
| cart       | [X]   | [X%]       |
| purchase   | [X]   | [X%]       |

- View → Cart conversion: **[X%]**
- Cart → Purchase conversion: **[X%]**

### Revenue
- Total revenue from purchase events: **[X]**
- Top brand by revenue: **[brand]** ([X])
- Top category by purchases: **[category]**

### User Behavior
- Peak activity hour: **[HH:00]**
- Users with cart events but no purchase: **[X]** ([X%] of cart users)
- Most active user: **[X events]**

### Takeaways
1. [Ek line: sabse bada pattern jo dikha]
2. [Ek line: drop-off kahan hai]
3. [Ek line: business recommendation]

## 8. Results
> Screenshots of the script output. Replace the placeholder text under each image with your own observation.

### Event Type Distribution
![Event distribution](screenshots/01_event_distribution.png)

Add one line: which event type is highest and what it says about the funnel.

### Top Brands by Purchases
![Top brands by purchases](screenshots/02_top_brands_purchases.png)

Add one line about the top brand.

### Top Brands by Revenue
![Top brands by revenue](screenshots/03_top_brands_revenue.png)

Add one line about the top brand by revenue.

### Hourly Activity
![Hourly activity](screenshots/04_hourly_activity.png)

Add one line about peak hours (timestamps are in UTC).

### Total Revenue
![Total revenue](screenshots/05_total_revenue.png)

### Parquet Output in HDFS
![Parquet output](screenshots/06_parquet_output.png)

Output of `hdfs dfs -ls` and `spark.read.parquet(...).show()` confirming the result was saved.

## 9. Spark SQL

A temporary view is created from the DataFrame:

```python
df.createOrReplaceTempView("ecommerce_behavior")
```

The same event, product, brand, and revenue analyses are also run with `spark.sql(...)`, and the results were cross-checked against the DataFrame API versions. Example:

```sql
SELECT brand, SUM(price) AS total_revenue
FROM ecommerce_behavior
WHERE event_type = 'purchase' AND brand IS NOT NULL
GROUP BY brand
ORDER BY total_revenue DESC
LIMIT 10
```

The queries are also collected in:

```text
queries/analysis_queries.sql
```

## 10. Output

Brand-wise purchase revenue is saved in **Parquet format**:

```text
/user/aaqib/output_projects/5_customer_behavior/brand_revenue_parquet/
```

The output is a folder containing `_SUCCESS` and one or more `part-*.parquet` files. Read it back with Spark:

```python
spark.read.parquet("hdfs://localhost:9000/user/aaqib/output_projects/5_customer_behavior/brand_revenue_parquet").show()
```

## 11. Design Decisions and Data Quality

- **Null handling:** `brand` and `category_code` contain null values. Rows with null values are excluded from brand and category analyses (the null counts are printed at the start of the script).
- **Parquet output:** Parquet is columnar, compressed, and keeps the schema with the data, so it is fast to read and easy to use from BI tools or later Spark jobs.
- **Only brand revenue is persisted:** the other analyses are exploratory and displayed on the console (see Results). Brand revenue is the key business metric and is small enough to store as a reusable output.
- **Caching:** the DataFrame is cached to avoid re-reading the CSV from HDFS for every analysis.
- **Timestamps** are in UTC, so hourly activity is shown in UTC.

## 12. Project Structure

```text
.
├── data_sets/
│   └── ecommerce_behavior/
│       └── 2019-Oct.csv
├── docs/
│   └── project_documentation.md
├── queries/
│   └── analysis_queries.sql
├── screenshots/
├── spark_scripts/
│   └── behavior_analysis.py
└── README.md
```

## 13. How to Run

Run these commands from the project root directory.

**Step 1: Upload the dataset to HDFS**

```bash
hdfs dfs -mkdir -p /user/aaqib/input_projects/5_customer_behavior

hdfs dfs -put data_sets/ecommerce_behavior/2019-Oct.csv \
/user/aaqib/input_projects/5_customer_behavior/
```

If the file already exists in HDFS, check first to avoid a duplicate-file error:

```bash
hdfs dfs -ls /user/aaqib/input_projects/5_customer_behavior/
```

**Step 2: Run the PySpark application**

```bash
spark-submit --master local[2] --driver-memory 2g spark_scripts/behavior_analysis.py
```

To also save the console output to a log file:

```bash
spark-submit --master local[2] --driver-memory 2g spark_scripts/behavior_analysis.py 2>&1 | tee output_log.txt
```

**Step 3: Check the output in HDFS**

```bash
hdfs dfs -ls /user/aaqib/output_projects/5_customer_behavior/brand_revenue_parquet/
```

## 14. Key Learning Outcomes

- Working with a large e-commerce dataset using PySpark
- Reading CSV data from HDFS
- DataFrame filtering, grouping, aggregation, and sorting
- Using Spark SQL with a temporary view
- Handling null values in analysis
- Customer, product, brand, category, session, and revenue analysis
- Writing aggregated results to HDFS in Parquet format

## 15. Conclusion

This project demonstrates an e-commerce analytics workflow using PySpark and HDFS. It applies DataFrame and Spark SQL transformations to customer interaction data and writes brand-wise purchase revenue to Parquet files in HDFS.
