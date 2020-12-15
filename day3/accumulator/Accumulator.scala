// Databricks notebook source
val datardd = sc.textFile("/FileStore/tables/StackOverflow_survey_responses.csv")

// COMMAND ----------

// DBTITLE 1,Accumulator variable
val total_rows = sc.longAccumulator //total rows

val midMissingSalary = sc.longAccumulator //mising salary

// COMMAND ----------

// put the logic

val afghanistanrdd = datardd.filter( responseData => {
  val splitData = responseData.split(",",-1)
  total_rows.add(1)
  
  if (splitData(14).isEmpty){
    midMissingSalary.add(1)
  }
  splitData(2) == "Afghanistan"
})

// COMMAND ----------

print(total_rows.value)

// COMMAND ----------

afghanistanrdd.take(3)

// COMMAND ----------

print(total_rows.value)

// COMMAND ----------


