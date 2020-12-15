// Databricks notebook source
val data = sc.textFile("/FileStore/tables/Property_data.csv")

// COMMAND ----------

// DBTITLE 1,removing blank data
val removeHeader =data.filter(line => !line.contains("Price"))
removeHeader.take(10)

// COMMAND ----------

// DBTITLE 1,taking bedroom and price column
val roomrdd = removeHeader.map(x=> (x.split(",")(3).toInt,(1, x.split(",")(2).toDouble)))
roomrdd.collect()

// COMMAND ----------

// DBTITLE 1,time of occurance and total
val reducerdd = roomrdd.reduceByKey ( (x,y) => (x._1 + y._1, x._2+y._2))
reducerdd.take(10)

// COMMAND ----------

// DBTITLE 1,Average
val finalrdd = reducerdd.mapValues(data => data._2/data._1)
finalrdd.collect()

// COMMAND ----------

// DBTITLE 1,for better printing
for ( (bedroom,avg)<- finalrdd.collect() )   println(bedroom + " : " + avg)

// COMMAND ----------

finalrdd.saveAsTextFile("Properryfile.csv")

// COMMAND ----------


