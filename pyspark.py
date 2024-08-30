# Databricks notebook source
# MAGIC %md
# MAGIC Read
# MAGIC the data from a CSV file into a PySpark DataFrame.
# MAGIC Perform
# MAGIC
# MAGIC

# COMMAND ----------

from pyspark.sql import SparkSession

spark=SparkSession.builder.appName("Ecommerce").getOrCreate()

# COMMAND ----------

from pyspark.sql.functions import when,col


# COMMAND ----------

data=spark.read.csv("/FileStore/tables/data1.csv",header=True,inferSchema=True)

# COMMAND ----------

# MAGIC %md
# MAGIC transformations to clean and prepare the data.
# MAGIC Calculate

# COMMAND ----------

# Show schema
data.printSchema()




# COMMAND ----------

# Show first few rows
data.show(5)

# COMMAND ----------

#droping unwanted column _c0
data=data.drop("_c0")
data


# COMMAND ----------

# Drop rows with missing values
data = data.dropna()

# COMMAND ----------

# MAGIC %md
# MAGIC aggregates such as total sales per category and customer segmentation.
# MAGIC

# COMMAND ----------

#total sales per category
category_sales = data.groupBy('Product_Category') \
    .agg(
        {'Total_Sales': 'sum', 'quantity_sold': 'sum'}
    ) \
    .withColumnRenamed('sum(Total_Sales)', 'Total_Sales') \
    .withColumnRenamed('sum(quantity_sold)', 'total_quantity_sold')

# Show the results
category_sales.show()


# COMMAND ----------

#total sales per customer segmentation
customer_segments = data \
    .withColumn('segment',
                when(col('Total_Sales') > 5000, 'High Value')
                .when((col('Total_Sales') <= 5000) & (col('Total_Sales') > 1000), 'Medium Value')
                .otherwise('Low Value')
               )

# Show the results
customer_segments.display()


# COMMAND ----------

# MAGIC %md
# MAGIC Store
# MAGIC the processed data back into a distributed file system.

# COMMAND ----------

# Define paths to save the results
category_sales_path = '/FileStore/tables/category_sales.parquet'
customer_segments_path = '/FileStore/tables/customer_segments.parquet'

# Save the DataFrames to Parquet files
category_sales.write.parquet(category_sales_path, mode='overwrite')
customer_segments.write.parquet(customer_segments_path, mode='overwrite')

