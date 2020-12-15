// Databricks notebook source
val julyrdd = sc.textFile("/FileStore/tables/nasa_july-2.tsv")
val augustrdd = sc.textFile("/FileStore/tables/nasa_august-2.tsv")

// COMMAND ----------

val unionrdd = julyrdd.union(augustrdd)
unionrdd.take(3)

// COMMAND ----------

// DBTITLE 1,removing column name
val header = unionrdd.first

// COMMAND ----------

unionrdd.filter(line => line!=header).take(3)


// COMMAND ----------

// DBTITLE 1,2nd approach
def headerRemover(line: String): Boolean =!(line.startsWith("host"))

// COMMAND ----------


