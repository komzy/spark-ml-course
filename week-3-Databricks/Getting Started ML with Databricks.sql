-- Databricks notebook source
-- MAGIC %md
-- MAGIC 
-- MAGIC # Welcome to the course: Machine Learning with Spark (Session 5)
-- MAGIC 
-- MAGIC This course is offered by in collaboration of  [AI-LOUNG](http://ai-lounge.com/) & [Datafy2AI](https://www.datafy2ai.com/)
-- MAGIC 
-- MAGIC Instructor: [Dr. Muhammad Amjad Raza](https://www.linkedin.com/in/amjadraza/)
-- MAGIC 
-- MAGIC ## Refresher:
-- MAGIC 
-- MAGIC 
-- MAGIC 1. [x]  What is Spark?
-- MAGIC 2. [x]  Setting Up Spark within Google Colab Environment
-- MAGIC 3. [x]  Basics of Spark Operations?
-- MAGIC 4. [x]  Spark DataFrame API using PySpark
-- MAGIC 5. [x] Basics of Machine Learning?
-- MAGIC 6. [x] DataPipelines with Spark
-- MAGIC 7. [x] Feature Engineering with Spark
-- MAGIC 8. [x] Feature Engineering with KOALAS
-- MAGIC 9. [x] Machine Leraning with Spark
-- MAGIC 
-- MAGIC 
-- MAGIC ## Learning Objective of this Session:
-- MAGIC 
-- MAGIC 1. Getting Strated with Databricks
-- MAGIC 2. Loading Data Into Databricks and Create Tables for Persistence
-- MAGIC 3. Spark Example with Databricks 
-- MAGIC 4. Machine Learning Pipeline with example

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Databricks in 5 minutes

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ## What and Why Databricks
-- MAGIC 
-- MAGIC Let us go quickly through slide prepared by Ali (Found of Databricks)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Create a quickstart cluster
-- MAGIC 
-- MAGIC 1. In the sidebar, right-click the **Clusters** button and open the link in a new window.
-- MAGIC 1. On the Clusters page, click **Create Cluster**.
-- MAGIC 1. Name the cluster **Quickstart**.
-- MAGIC 1. In the Databricks Runtime Version drop-down, select **7.3 LTS (Scala 2.12, Spark 3.0.1)**.
-- MAGIC 1. Click **Create Cluster**.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Attach the notebook to the cluster and run all commands in the notebook
-- MAGIC 
-- MAGIC 1. Return to this notebook. 
-- MAGIC 1. In the notebook menu bar, select **<img src="http://docs.databricks.com/_static/images/notebooks/detached.png"/></a> > Quickstart**.
-- MAGIC 1. When the cluster changes from <img src="http://docs.databricks.com/_static/images/clusters/cluster-starting.png"/></a> to <img src="http://docs.databricks.com/_static/images/clusters/cluster-running.png"/></a>, click **<img src="http://docs.databricks.com/_static/images/notebooks/run-all.png"/></a> Run All**.

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ## The next command creates a table from a Databricks dataset

-- COMMAND ----------

DROP TABLE IF EXISTS diamonds;

CREATE TABLE diamonds
USING csv
OPTIONS (path "/databricks-datasets/Rdatasets/data-001/csv/ggplot2/diamonds.csv", header "true")


-- COMMAND ----------

SELECT * from diamonds

-- COMMAND ----------

-- MAGIC %python
-- MAGIC diamonds = spark.read.csv("/databricks-datasets/Rdatasets/data-001/csv/ggplot2/diamonds.csv", header="true", inferSchema="true")
-- MAGIC diamonds.write.format("delta").save("/delta/diamonds")

-- COMMAND ----------

DROP TABLE IF EXISTS diamonds;

CREATE TABLE diamonds USING DELTA LOCATION '/delta/diamonds/'

-- COMMAND ----------

SELECT * from diamonds

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## The next command manipulates the data and displays the results 
-- MAGIC 
-- MAGIC Specifically, the command:
-- MAGIC 1. Selects color and price columns, averages the price, and groups and orders by color.
-- MAGIC 1. Displays a table of the results.

-- COMMAND ----------

SELECT color, avg(price) AS price FROM diamonds GROUP BY color ORDER BY color

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Convert the table to a chart
-- MAGIC 
-- MAGIC Under the table, click the bar chart <img src="http://docs.databricks.com/_static/images/notebooks/chart-button.png"/></a> icon. 

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ## Repeat the same operations using Python DataFrame API. 
-- MAGIC This is a SQL notebook; by default command statements are passed to a SQL interpreter. To pass command statements to a Python interpreter, include the `%python` magic command.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## The next command creates a DataFrame from a Databricks dataset

-- COMMAND ----------

-- MAGIC %python
-- MAGIC diamonds = spark.read.csv("/databricks-datasets/Rdatasets/data-001/csv/ggplot2/diamonds.csv", header="true", inferSchema="true")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## The next command manipulates the data and displays the results

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import avg
-- MAGIC 
-- MAGIC display(diamonds.select("color","price").groupBy("color").agg(avg("price")).sort("color"))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Loading Data into Databrick 
-- MAGIC 
-- MAGIC * Download data into csv format
-- MAGIC * Trim the data if it is too big (with community edition you have some restrictions)
-- MAGIC * Upload the data using `Data` tool in left hand Pan
-- MAGIC 
-- MAGIC ## Steps to Follow
-- MAGIC 
-- MAGIC 1. Using `Data` tool, click on the `create table` button
-- MAGIC 2. Make sure your cluster is running and active
-- MAGIC 3. Select the `.csv` file from your local computer
-- MAGIC 4. Upload it and wait for it to be finish uploading
-- MAGIC 5. Using the build-in tool, Slecect the correct header/column name for your data set using check box
-- MAGIC 6. Infer the Schma using Check box
-- MAGIC 7. Using Notebook, locad the data and save tempView as well as permanent view in `.parquet` format.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Machine Learning Pipeline Example
-- MAGIC 
-- MAGIC Since, this course is machine learning with Spark, Let us go through a complete machine learning pipeline using databricks environment
