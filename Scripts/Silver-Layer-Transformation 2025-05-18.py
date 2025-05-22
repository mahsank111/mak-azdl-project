# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC # Silver Layer Script

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Access using mak-poject-app

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.makstoragedatalake.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.makstoragedatalake.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.makstoragedatalake.dfs.core.windows.net", "abc4af58-e2a4-4933-9edf-43c04a99471b")
spark.conf.set("fs.azure.account.oauth2.client.secret.makstoragedatalake.dfs.core.windows.net", "jhp8Q~xk-qbnn~OADWdo-a3oISHrD8fG1igexaq3")
spark.conf.set("fs.azure.account.oauth2.client.endpoint.makstoragedatalake.dfs.core.windows.net", "https://login.microsoftonline.com/2ef0e1df-e30f-44bd-b4ee-1c54e9a8a724/oauth2/token")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Loading

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read Calender Data (AdventureWorks_Calendar)

# COMMAND ----------

df_cal = spark.read.format('csv')\
            .option("header", True)\
            .option("inferSchema", True)\
            .load('abfss://bronze@makstoragedatalake.dfs.core.windows.net/AdventureWorks_Calendar')

# COMMAND ----------

df_cal.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Loading Every Data one by one (DATA READ)

# COMMAND ----------

df_cus = spark.read.format('csv')\
        .option("header", True)\
        .option("inferSchema", True)\
        .load('abfss://bronze@makstoragedatalake.dfs.core.windows.net/AdventureWorks_Customers')

# COMMAND ----------

df_prodcat = spark.read.format('csv')\
        .option("header", True)\
        .option("inferSchema", True)\
        .load('abfss://bronze@makstoragedatalake.dfs.core.windows.net/AdventureWorks_Product_Categories')


# COMMAND ----------

df_prod = spark.read.format('csv')\
        .option("header", True)\
        .option("inferSchema", True)\
        .load('abfss://bronze@makstoragedatalake.dfs.core.windows.net/AdventureWorks_Products')

# COMMAND ----------

df_ret = spark.read.format('csv')\
        .option("header", True)\
        .option("inferSchema", True)\
        .load('abfss://bronze@makstoragedatalake.dfs.core.windows.net/AdventureWorks_Returns')

# COMMAND ----------

df_sales = spark.read.format('csv')\
        .option("header", True)\
        .option("inferSchema", True)\
        .load('abfss://bronze@makstoragedatalake.dfs.core.windows.net/AdventureWorks_Sales*')

# COMMAND ----------

df_ter = spark.read.format('csv')\
        .option("header", True)\
        .option("inferSchema", True)\
        .load('abfss://bronze@makstoragedatalake.dfs.core.windows.net/AdventureWorks_Territories')

# COMMAND ----------

df_subcat = spark.read.format('csv')\
        .option("header", True)\
        .option("inferSchema", True)\
        .load('abfss://bronze@makstoragedatalake.dfs.core.windows.net/Product_Subcategories')

# COMMAND ----------

# MAGIC %md
# MAGIC ## TRANSFORMATION

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data - Calender 

# COMMAND ----------

df_cal.display()


# COMMAND ----------

df_cal = df.withColumn('Month',month(col('Date')))\
           .withColumn('Year',year(col('Date')))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write Data Modes (append(), overwrite(), error(), ignore())

# COMMAND ----------

df_cal.write.format('parquet')\
        .mode('append')\
        .option("path","abfss://silver@makstoragedatalake.dfs.core.windows.net/AdventureWorks_Calendar")\
        .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data - Customers

# COMMAND ----------

df_cus.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### In This Table we can transform so many things (huge size)
# MAGIC
# MAGIC ### First : Text/string Tranformation (have Prefix, First Name, Last Name 
# MAGIC
# MAGIC ## Lets Add one new col - Full Name 'FullName'
# MAGIC
# MAGIC

# COMMAND ----------

df_cus = df_cus.withColumn("FullName",concat(col("FirstName"),lit(" "),col("LastName")))
df_cus.display()
df_cus.write.format('parquet')\
        .mode('append')\
        .option("path","abfss://silver@makstoragedatalake.dfs.core.windows.net/AdventureWorks_Customers")\
        .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data - SubCategories

# COMMAND ----------

df_subcat.display()

# COMMAND ----------

df_subcat.write.format('parquet')\
        .mode('append')\
        .option("path","abfss://silver@makstoragedatalake.dfs.core.windows.net/AdventureWorks_Subcategories")\
        .save() 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data - Products Display to chaeck what we can Transform

# COMMAND ----------

df_prod.display()     

# COMMAND ----------

# MAGIC %md
# MAGIC ### Lets modify the above table's (ProductSKU) Col in a way that we can return the 2 Letter before (-) from that. 

# COMMAND ----------

df_prod = df_prod.withColumn("ProductSKU", split(col("ProductSKU"), '-')[0]) \
                 .withColumn('ProductName', split(col("ProductName"), ' ')[0])

# COMMAND ----------

df_prod.display()

# COMMAND ----------

df_prod.write.format('parquet')\
        .mode('append')\
        .option("path","abfss://silver@makstoragedatalake.dfs.core.windows.net/AdventureWorks_Products")\
        .save() 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data - _Returns_

# COMMAND ----------

df_ret.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Not much to transfrom, so writing it directly into #SILVER 

# COMMAND ----------

df_ret.write.format('parquet')\
        .mode('append')\
        .option("path","abfss://silver@makstoragedatalake.dfs.core.windows.net/AdventureWorks_Returns")\
        .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data - _Territories_

# COMMAND ----------

df_ter.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Not much to transfrom, so writing it directly into #SILVER 

# COMMAND ----------

df_ter.write.format('parquet')\
        .mode('append')\
        .option("path","abfss://silver@makstoragedatalake.dfs.core.windows.net/AdventureWorks_Territories")\
        .save()

# COMMAND ----------

df_sales.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Sales data - could be transformed well as consists many cols 
# MAGIC
# MAGIC **1. StockDate',to_timestamp**
# MAGIC
# MAGIC **2. OrderNumber',regexp_replace('OrderNumber','S','T')**
# MAGIC
# MAGIC **3. df_sales.withColumn('Multiply',col('OrderLineItem') x col('OrderQuantity')**

# COMMAND ----------

df_sales = df_sales.withColumn('StockDate',to_timestamp('StockDate'))

# COMMAND ----------

df_sales = df_sales.withColumn('OrderNumber',regexp_replace('OrderNumber','S','T'))

# COMMAND ----------

df_sales =  df_sales.withColumn('Multiply',col('OrderLineItem')*col('OrderQuantity'))

# COMMAND ----------

df_sales.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Sales Analysis on above sales data 

# COMMAND ----------

df_sales.groupBy('OrderDate').agg(count('OrderNumber').alias('TotalOrders')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Sales Analysis - Via Visualisations on ProductCategoryTable
# MAGIC ### 
# MAGIC ### Here checking composition like which category is performing well✨

# COMMAND ----------

df_prodcat.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Visualising Territories Data with bar chart**

# COMMAND ----------

df_ter.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Writing Sales Data into #silver layer**

# COMMAND ----------

df_sales.write.format('parquet')\
        .mode('append')\
        .option("path","abfss://silver@makstoragedatalake.dfs.core.windows.net/AdventureWorks_Sales")\
        .save()

# COMMAND ----------

