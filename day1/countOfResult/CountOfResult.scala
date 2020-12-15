// Databricks notebook source
val julydata = sc.textFile("/FileStore/tables/nasa_july.tsv")
val augustdata = sc.textFile("/FileStore/tables/nasa_august.tsv")

// COMMAND ----------

julydata.collect()

// COMMAND ----------

augustdata.collect()

// COMMAND ----------

val julyHost = julydata.map(x=>x.split("\t")(0))
val augustHost = augustdata.map(x=>x.split("\t")(0))

// COMMAND ----------

var intersectionRDD = julyHost.intersection(augustHost)

// COMMAND ----------

def headerRemover(line: String):Boolean = !(line.startsWith("host"))
intersectionRDD.filter(x=>headerRemover(x))count()

// COMMAND ----------


