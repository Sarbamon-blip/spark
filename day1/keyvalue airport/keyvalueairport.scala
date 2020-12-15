// Databricks notebook source
val data =sc.textFile("/FileStore/tables/airports-1.text")

// COMMAND ----------

data.map( line => line.split(",")).collect()

// COMMAND ----------

val datardd = data.map(line=> (line.split(",")(1),line.split(",")(3)))
datardd.take(3)

// COMMAND ----------

// DBTITLE 1,airport which is not canada 
val other_rdd_canada_except = datardd.filter(x=>x._2 != "\"canada\"")
other_rdd_canada_except.take(2)

// COMMAND ----------


