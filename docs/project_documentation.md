# Customer Behavior Analysis using Spark

## Project Description
This project analyzes large-scale e-commerce customer behavior using Apache Spark (PySpark).
The objective is to understand user interactions such as product views, add-to-cart actions, and purchases, and derive insights like customer engagement, popular products, and revenue trends.

This project simulates real-world big data processing using distributed computing.

## Technologies Used
- Apache Spark (PySpark)
- Python
- Linux (Ubuntu)
- HDFS (for distributed storage)
- CSV Dataset

## Dataset
The dataset contains e-commerce user interaction data with the following fields:

- `event_time` – Timestamp of the event 
- `event_type` – Type of action (view, cart, purchase) 
- `product_id` – Unique product identifier
- `category_id` – Product category ID
- `category_code` – Product category name 
- `brand` – Brand name 
- `price` – Product price
- `user_id` – Unique user identifier
- `user_session` – Session ID of the user

## Analysis Performed
The following analyses were performed using Spark:

- Event type distribution (view, cart, purchase)
- Top viewed products
- Top purchased products
- Top brands based on purchases
- Revenue generation analysis
- Active users analysis
- Session-based activity tracking
  
## Output
- Aggregated insights are generated using Spark DataFrame operations
- Key results are stored in the `output/` directory
- Outputs include:
  - Brand revenue analysis (stored in HDFS)
  - Other insights such as event counts, product rankings, and user analysis are displayed during execution

## Key Highlights
- Handles large-scale data using distributed processing
- Uses Spark transformations and actions efficiently
- Demonstrates real-world data engineering + analytics workflow
