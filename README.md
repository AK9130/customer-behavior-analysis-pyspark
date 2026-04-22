# Customer Behavior Analysis using Apache Spark

## Overview

This project analyzes large-scale e-commerce customer behavior using Apache Spark (PySpark).
It processes user interaction data (views, cart, purchases) to extract insights such as product popularity, customer activity, and revenue trends.

This project simulates a real-world big data analytics use case.

## Tech Stack

* Apache Spark (PySpark)
* Python
* Hadoop HDFS
* Linux

## Project Structure

```
.
├── data_sets
│   └── ecommerce_behavior
│       ├── 2019-Nov.csv
│       └── 2019-Oct.csv
├── docs
│   └── project_documentation.md
├── output
│   └── brand_revenue
│       ├── part-00000-4a20e486-9c5b-4da7-97a8-ef36d02fceba-c000.csv
│       ├── part-00001-4a20e486-9c5b-4da7-97a8-ef36d02fceba-c000.csv
│       └── _SUCCESS
├── queries
│   └── analysis_queries.sql
├── README.md
└── spark_scripts
    └── behavior_analysis.py
```

## Workflow

CSV → HDFS → Spark Processing → Data Analysis → Output (HDFS)

## How to Run

```bash
# Upload data to HDFS
hdfs dfs -put data_sets/ecommerce_behavior/*.csv /user/aaqib/input_projects/5_customer_behavior/

# Run Spark job
spark-submit spark_scripts/behavior_analysis.py
```

## Analysis Performed

* Event type distribution (view, cart, purchase)
* Top viewed and purchased products
* Brand-wise revenue analysis
* Most active users and sessions
* Cart abandonment analysis
* Hourly activity trends

## Outcome

Extracted meaningful insights from large-scale e-commerce data using distributed processing with Spark.

## Key Learning

Spark transformations, actions, distributed data processing, and real-world analytics.
