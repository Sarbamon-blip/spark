// Databricks notebook source
val julyrdd = sc.textFile("/FileStore/tables/nasa_july-2.tsv")
val augustrdd = sc.textFile("/FileStore/tables/nasa_august-2.tsv")

// COMMAND ----------

julyrdd.union(augustrdd).take(3)

// COMMAND ----------


