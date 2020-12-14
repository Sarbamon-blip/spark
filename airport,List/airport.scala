// Databricks notebook source
val data = sc.textFile("/FileStore/tables/airports.text")

// COMMAND ----------

data.collect()

// COMMAND ----------

// DBTITLE 1,creating key value pair
val datardd = data.map( line =>( line.split(",")(0), line.split(",")(4) ) )
datardd.take(3)

// COMMAND ----------

//tuple<= dictionary like for holding value

// COMMAND ----------

// DBTITLE 1,Airport not in canada
val other_rdd_canada_except = datardd.filter( x => x._2 != "\"Canada\"")
other_rdd_canada_except.take(10)

// COMMAND ----------

// DBTITLE 1,Array empty show the answer is correct
other_rdd_canada_except.filter( x => x._2 == "\"Canada\"").take(1)

// COMMAND ----------

// DBTITLE 1,List
val listData= List("sarbamon 1997","sertabon 1994","rajshree 1989","kamir 1969")

// COMMAND ----------

// DBTITLE 1,mapvalues
val newrdd =sc.parallelize(listData)

// COMMAND ----------

val rddnew =newrdd.map ( x => (x.split(" ")(0),x.split(" ")(1).toInt))
rddnew.take(3)                               

// COMMAND ----------

rddnew.mapValues(x=>x+10).take(3)

// COMMAND ----------


