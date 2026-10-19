# Customer Behavior Analysis Using Apache Spark
## 1. Project Introduction
This project analyzes e-commerce customer behavior data using Apache Spark (PySpark). It examines product views, cart additions, purchases,
customer activity, sessions, categories, brands, and revenue.

The data is read from Hadoop Distributed File System (HDFS), processed with PySpark, and selected analysis results are saved back to HDFS in
Parquet format.

## 2. Problem Statement
E-commerce platforms generate large volumes of customer interaction events. This project uses Spark to summarize those events and identify
patterns such as popular products, frequently purchased brands, customer activity, and purchase revenue.

## 3. Dataset
The local dataset directory contains:

``` text
data_sets/ecommerce_behavior/
├── 2019-Oct.csv
```

HDFS Input:
/user/aaqib/input_projects/5_customer_behavior/

Files:
- 2019-Oct.csv

The dataset includes these fields:

  Field             Description
  ----------------- ---------------------------------------------------
  `event_time`      Timestamp of the customer event
  `event_type`      Event type, such as `view`, `cart`, or `purchase`
  `product_id`      Product identifier
  `category_id`     Category identifier
  `category_code`   Product category
  `brand`           Product brand
  `price`           Product price
  `user_id`         Customer identifier
  `user_session`    Session identifier

## 4. Technologies Used
-   Python
-   Apache Spark
-   PySpark DataFrames
-   Spark SQL
-   Hadoop HDFS
-   Linux (Ubuntu)
-   CSV input and Parquet output

## 5. Project Architecture
``` text
E-commerce CSV files
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
        +-----------------------+
        |                       |
        v                       v
 DataFrame analysis        Temporary SQL view
        |                       |
        +-----------+-----------+
                    |
                    v
             Analysis results
                    |
                    v
        Brand revenue in Parquet
                    |
                    v
                   HDFS
```

## 6. Analysis Performed
The script includes the following analyses:

1.  Count of events by event type
2.  Top 10 viewed products
3.  Top 10 purchased products
4.  Top 10 brands by purchase-event count
5.  Revenue by brand for purchase events
6.  Most active users by event count
7.  Top products added to cart
8.  Top categories by purchase-event count
9.  Average price by category
10. Total number of distinct users
11. Most active sessions by event count
12. Hourly event activity
13. Highest-priced purchase events
14. Users with cart events but no purchase event anywhere in the loaded
    data
15. Total revenue from purchase events

The script also saves brand-wise purchase revenue to HDFS in Parquet
format.

## 7. Project Structure

``` text
.
├── data_sets/
│   └── ecommerce_behavior/
│       ├── 2019-Oct.csv
├── docs/
│   └── project_documentation.md
├── output/
│   └── brand_revenue/
├── queries/
│   └── analysis_queries.sql
├─ spark_scripts/
│   └── behavior_analysis.py
└── README.md
```

## 8. HDFS Paths
**Input directory**

``` text
/user/aaqib/input_projects/5_customer_behavior/
```

**Output directory**

``` text
/user/aaqib/output_projects/5_customer_behavior/brand_revenue_parquet/
```

## 9. How to Run
Run these commands from the project root directory.

### Step 1: Upload the dataset files to HDFS

``` bash
hdfs dfs -mkdir -p /user/aaqib/input_projects/5_customer_behavior
hdfs dfs -put data_sets/ecommerce_behavior/2019-Oct.csv /user/aaqib/input_projects/5_customer_behavior/
```

If the files already exist in HDFS, check the directory first to avoid a
duplicate-file error:

``` bash
hdfs dfs -ls /user/aaqib/input_projects/5_customer_behavior/
```

### Step 2: Run the PySpark script

``` bash
spark-submit spark_scripts/behavior_analysis.py
```

### Step 3: Check the output in HDFS

``` bash
hdfs dfs -ls /user/aaqib/output_projects/5_customer_behavior/brand_revenue_parquet/
```

## 10. Spark SQL Queries
The SQL examples are stored in:

``` text
queries/analysis_queries.sql
```

The script creates a temporary view named `ecommerce_behavior` using:

``` python
df.createOrReplaceTempView("ecommerce_behavior")
```

The SQL queries can be run through `spark.sql(...)` in a Spark session
where this temporary view has been created. The `.sql` file is a
collection of query examples; it is not executed automatically by the
current Python script.

## 11. Key Learning Outcomes
Through this project, I practised:

-   Reading CSV data from HDFS with PySpark
-   Creating and working with Spark DataFrames
-   Filtering records by event type
-   Grouping and aggregating data
-   Sorting results and selecting top records
-   Creating a Spark SQL temporary view
-   Performing user, product, brand, category, and session analysis
-   Calculating revenue from purchase events
-   Writing aggregated results to HDFS in Parquet format

## 12. Conclusion
This project demonstrates an e-commerce analytics workflow using PySpark and HDFS. It applies DataFrame transformations and aggregations to
customer interaction data and writes brand-wise purchase revenue to Parquet files in HDFS.
