# Customer Behavior Analysis using Spark

## Project Description

- This project analyzes customer behavior on an e-commerce website using Apache Spark (PySpark).
- It analyzes views, cart additions, and purchases to understand product popularity, customer activity, and revenue.
- The project demonstrates large-scale data processing using Spark and HDFS.

## Technologies Used

- Apache Spark (PySpark)
- Spark SQL
- Python
- Linux (Ubuntu)
- Hadoop HDFS
- CSV Dataset

## Dataset

The dataset contains e-commerce customer activity with the following fields:

- `event_time` – event timestamp
- `event_type` – view, cart, or purchase
- `product_id` – product ID
- `category_id` – category ID
- `category_code` – product category
- `brand` – product brand
- `price` – product price
- `user_id` – customer ID
- `user_session` – session ID

## Analysis Performed

- Event type distribution
- Top viewed and purchased products
- Top brands and brand-wise revenue
- Most active users and sessions
- Top categories and average category price
- Unique users and hourly activity
- Highest-value purchases
- Cart abandonment analysis
- Total revenue analysis

## Output

- Analysis results are displayed using Spark DataFrame and Spark SQL operations.
- Brand-wise revenue is stored in HDFS.
- Output is generated under the project output directory.

## Key Highlights

- Large-scale e-commerce data processing using Spark.
- Uses PySpark, Spark SQL, and HDFS.
- Performs product, customer, session, and revenue analysis.
- Demonstrates a real-world big data analytics workflow.
