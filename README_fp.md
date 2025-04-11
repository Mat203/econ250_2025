# Final Project Overview

This is a Brazilian ecommerce public dataset of orders made at Olist Store. The dataset has information of 100k orders from 2016 to 2018 made at multiple marketplaces in Brazil.

## Technical overview

Our project is built on dbt and is centered around transforming and analyzing e‑commerce data. We integrate data from multiple raw source tables into a sales model, and then derive a series of analytical models for business insights related to order performance, payment trends, seller performance, and customer behavior.

## Data Sources

Data is collected from csv-files via Kaggle archive and passes initial tests on unique values for id and relations rows which are foreign keys (it is provided in fp_sources.yml)

## Staging models

1. stg_fp_customers
Cleans and casts customer fields (e.g., city, state)

2. stg_fp_order_items
Casts numeric values (price, freight_value)

3. stg_fp_order_payments
Converts payment fields to proper numeric types and cleans payment type values.

4. stg_fp_orders:
Casts timestamps and computes various time-based metrics, such as:

    delivery_duration_days – days between purchase and actual delivery.

    is_shipped – a flag indicating whether an order is delivered.

5. stg_fp_products:
Converts dimensions to integers and computes product_volume (length × height × width)

6. stg_fp_sellers:
Cleans seller data.

7. stg_fp_product_category_name_translation:
Passes through translation mappings for product category names to English.

## Denormalized Sales Model (fp_sales_full)

The fp_sales_full model aggregates and denormalizes data at an order level. Each row in this model represents one order along with the following:

1. Base order information from stg_fp_orders.

2. A nested array of order items (aggregated with BigQuery’s ARRAY and STRUCT), which contains product details and category translations.

3. A nested array of payment details aggregated from stg_fp_order_payments.

4. The table is partitioned by order_purchase_timestamp (with daily granularity) and clustered by order_status to optimize query performance.

## Analytical Models

1. Daily Order Performance 
Aggregates orders on a daily basis, grouping by date and order status, and computes metrics such as the number of orders and total revenue for the day.
2. Seller Performance Metrics
Aggregates seller-related data to compute the number of orders each seller fulfilled and efficiency metrics
3. Customer Behavior
Aggregates at the customer level, calculating total purchase frequency and lifetime value.
4. Product Performance
This model aggregates sales data at the product category level. For each category, it calculates the total revenue by summing the product price and the corresponding freight costs for each order item

## Custom Data Tests
1. Lifetime Value Consistency
A test that calculates each customer’s lifetime revenue from raw orders and compares it with the lifetime_value in the customer behavior mart model.
2. Order Count Consistency
Compares the count of unique orders in fp_sales_full with the total number of orders in an analytic model
