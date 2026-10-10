# Project Documentation: Customer Behavior Analysis Using Spark

## 1. Implementation Details
This project uses PySpark to process e-commerce customer behavior data stored in HDFS.
The script reads CSV files, creates a Spark DataFrame, and performs aggregations to analyze customer events, products, brands, categories, sessions, and revenue.

## 2. Data Processing
- Read CSV files from HDFS using spark.read.csv().
- Enable header recognition and schema inference.
- Create a temporary SQL view named ecommerce_behavior.
- Filter records by event type.
- Group records and calculate counts, averages, and revenue.
- Sort results to identify top products, brands, and users.
- Save brand-wise purchase revenue in Parquet format.

## 3. Important Technical Decisions
### Reading Multiple CSV Files

The input path uses a wildcard:

hdfs:///user/aaqib/input_projects/5_customer_behavior/*.csv

Spark reads matching CSV files from the specified directory. The actual dataset processed depends on which files are present there.

### Revenue Calculation
Revenue by brand is calculated using purchase events and the sum of the price column.

### Parquet Output
The script saves brand-wise revenue using the following code:

.write \
.mode("overwrite") \
.parquet("hdfs:///user/aaqib/output_projects/5_customer_behavior/brand_revenue")

The overwrite mode replaces existing output at the target path when the write runs successfully.

## 4. Cart and Purchase Analysis
The script compares distinct users with cart events against distinct users with purchase events.
This identifies users who had cart activity but no purchase event in the loaded dataset. It is a user-level comparison, not a session-level or product-level cart abandonment calculation.

## 5. Output Verification
The output can be checked using:

hdfs dfs -ls /user/aaqib/output_projects/5_customer_behavior/brand_revenue/

The directory contains Parquet part-files and a _SUCCESS marker after a successful write.

## 6. Challenges and Learning
- Reading multiple CSV files from HDFS.
- Working with large datasets using PySpark.
- Performing filtering, grouping, aggregation, and sorting.
- Creating and using a Spark SQL temporary view.
- Writing aggregated results in Parquet format.
- Verifying output files in HDFS.

## 7. Related Files
- PySpark script: spark_scripts/behavior_analysis.py
- Spark SQL queries: queries/analysis_queries.sql
- Main project overview: README.md
