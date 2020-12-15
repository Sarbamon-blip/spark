// Databricks notebook source
val remHeader =sc.textFile("/FileStore/tables/FriendsData.csv")

// COMMAND ----------

remHeader.take(10)
val frnd = remHeader.map(x => (x.split(",")(2).toInt,x.split(",")(3).toInt))



// COMMAND ----------

val initial = 0
val com_max = (x:Int,y:Int) => if(x>y) x else y
val merge_max =(p1:Int,p2:Int) =>if (p1>p2) p1 else p2
val aggrdd=frnd.aggregateByKey(initial)((com_max),(merge_max))

// COMMAND ----------


