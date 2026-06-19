# Databricks notebook source
# DBTITLE 1,Spark Config
spark.conf.set("fs.azure.account.key.Container.dfs.core.windows.net","Account_secret") 

# COMMAND ----------

# DBTITLE 1,Reading Order and Product Files
 #reading files from raw/bronze layer
 order_df = spark.read.option("header", "true").csv("abfss://source@container.dfs.core.windows.net/raw/orders.csv")
 product_df = spark.read.option("header", "true").csv("abfss://source@container.dfs.core.windows.net/raw/products.csv")

# COMMAND ----------

# creating temp table/view "order_table"
order_df.createOrReplaceTempView("order_table")

# COMMAND ----------

test_df=spark.sql("""
SELECT * FROM order_table
""")
display(test_df)

# COMMAND ----------

# DBTITLE 1,Order Silver(Cleaned, Transformed)
from pyspark.sql.functions import *

orders_silver = (
    order_df.dropDuplicates(["order_id"])
    .withColumn("order_date", to_date("order_date"))
    .withColumn("ship_date", to_date("ship_date"))
    .withColumn("quantity", col("quantity").cast("int"))
    .withColumn("total_amount", col("total_amount").cast("double"))
)


# COMMAND ----------

# DBTITLE 1,Product Silver(Cleaned, Transformed)
products_silver = (
    product_df
    .withColumn("unit_price",col("unit_price").cast("double"))
    .withColumn("cost_price",col("cost_price").cast("double"))
    .withColumn("stock_quantity",col("stock_quantity").cast("int"))
)

# COMMAND ----------

# DBTITLE 1,Join Order and Product
sales_df = (
    orders_silver.alias("o")
    .join(
        products_silver.alias("p"),
        col("o.product_id") == col("p.product_id"),
        "inner"
    ).select(
        "o.order_id",
        "o.customer_id",
        "o.customer_name",
        "o.email",
        "o.product_id",
        "o.quantity",
        "o.order_date",
        "o.ship_date",
        "o.order_status",
        "o.payment_method",
        "o.total_amount",
        "p.product_name",
        "p.category",
        "p.brand",
        "p.unit_price",
        "p.cost_price",
        "p.stock_quantity"
    
))
display(sales_df)

# COMMAND ----------

# DBTITLE 1,Another Option to do Join Using Spark SQL
# Create temporary views for orders and products silver tables
orders_silver.createOrReplaceTempView("orders_silver")
products_silver.createOrReplaceTempView("products_silver")

# Select sales data by joining orders and products silver tables
select_sales_df = spark.sql("""
SELECT
    o.order_id,
    o.customer_id,
    o.customer_name,
    o.email,
    o.product_id,
    o.quantity,
    o.order_date,
    o.ship_date,
    o.order_status,
    o.payment_method,
    o.total_amount,
    p.product_name,
    p.category,
    p.brand,
    p.unit_price,
    p.cost_price,
    p.stock_quantity
FROM orders_silver o
INNER JOIN products_silver p
    ON o.product_id = p.product_id
""")

select_sales_df.createOrReplaceTempView("sales_table")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from sales_table

# COMMAND ----------

# DBTITLE 1,Saved Sales Data(Model) To Target
sales_df.write \
    .format("csv") \
    .mode("overwrite") \
    .save("abfss://target@datalake010626.dfs.core.windows.net/model/sales_data_product/")

# COMMAND ----------

# DBTITLE 1,GOLD: Product Sales Summary
# Business KPI: Which products generate highest revenue?
# gold_product_sales = (
#     sales_df
#     .groupBy(
#         "product_id",
#         "product_name",
#         "category"
#     )
#     .agg(
#         sum("total_amount").alias("revenue"),
#         sum("quantity").alias("units_sold")
#     )
# )

gold_product_sales=spark.sql("""select product_id,product_name,category,sum(total_amount) as revenue,sum(quantity) as units_sold from sales_table group by product_id,product_name,category""")

gold_product_sales.write.mode("overwrite").saveAsTable("retail_sales_gold.gold_product_sales")


# COMMAND ----------

# DBTITLE 1,GOLD: Category Revenue Analysis
# Business KPI: Which category contributes most revenue?
# What is the total revenue and Units for each product category?
# gold_category_sales = (
#     sales_df
#     .groupBy("category")
#     .agg(
#         sum("total_amount").alias("revenue"),
#         sum("quantity").alias("units_sold")
#     )
# )

gold_category_sales=spark.sql("""select category,sum(total_amount) as revenue,sum(quantity) as units_sold from sales_table group by category""")
gold_category_sales.write.mode("overwrite").saveAsTable("retail_sales_gold.gold_category_sales")



# COMMAND ----------

# DBTITLE 1,Gold: Brand Performance
# Business KPI: Which Brand performs best?
# What is the total revenue for each brand?
# gold_brand_sales = (
#     sales_df
#     .groupBy("brand")
#     .agg(
#         sum("total_amount").alias("revenue"),
#         sum("quantity").alias("units_sold")
#     )
# )

gold_brand_sales=spark.sql("""select brand,sum(total_amount) as revenue,sum(quantity) as units_sold from sales_table group by brand""")
#gold_brand_sales.write.mode("overwrite").saveAsTable("retail_sales_gold.gold_brand_sales")
gold_brand_sales.write \
    .format("delta") \
    .mode("overwrite") \
    .save("abfss://target@datalake010626.dfs.core.windows.net/gold/brand_sales")


# COMMAND ----------

# DBTITLE 1,GOLD: Customer Lifetime Value (CLV)
# Business KPI: Which customers spend the most?
# gold_customer_spend = (
#     sales_df
#     .groupBy(
#         "customer_id",
#         "customer_name"
#     )
#     .agg(
#         sum("total_amount").alias("total_spent")
#     )
# )
gold_customer_spend=spark.sql("""select customer_id,customer_name,sum(total_amount) as total_spent from sales_table group by customer_id,customer_name""")

#customer_spend.write.mode("overwrite").saveAsTable("retail_sales_gold.customer_spend")
gold_customer_spend.write \
    .format("delta") \
    .mode("overwrite") \
    .save("abfss://target@datalake010626.dfs.core.windows.net/gold/customer_spend")

# COMMAND ----------

# DBTITLE 1,GOLD: Payment Method Analysis
# Business KPI: What is the total revenue for each payment method and Most used payment method?
# gold_payment_sales = (
#     sales_df
#     .groupBy("payment_method")
#     .agg(
#         sum("total_amount").alias("revenue"),
#         sum("quantity").alias("units_sold")
#     )
# )

gold_payment_sales=spark.sql("""select payment_method,sum(total_amount) as revenue,sum(quantity) as units_sold from sales_table group by payment_method""")

#payment_sales.write.mode("overwrite").saveAsTable("retail_sales_gold.gold_payment_sales")
gold_payment_sales.write \
    .format("delta") \
    .mode("overwrite") \
    .save("abfss://target@datalake010626.dfs.core.windows.net/gold/payment_sales")


# COMMAND ----------

# DBTITLE 1,GOLD:Order Status Analysis
# What is the total revenue for each order status?
# gold_order_status_sales = (
#     sales_df
#     .groupBy("order_status")
#     .agg(
#         sum("total_amount").alias("revenue"),
#         sum("quantity").alias("units_sold")
#     )
# )

gold_order_status_sales=spark.sql("""select order_status,sum(total_amount) as revenue,sum(quantity) as units_sold from sales_table group by order_status""")

#gold_order_status_sales.write.mode("overwrite").saveAsTable("retail_sales_gold.gold_order_status_sales")

gold_order_status_sales.write \
    .format("delta") \
    .mode("overwrite") \
    .save("abfss://target@datalake010626.dfs.core.windows.net/gold/order_status_sales")

