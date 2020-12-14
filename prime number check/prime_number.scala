// Databricks notebook source
// DBTITLE 1,sum of prime number

val prime_num=sc.textFile("/FileStore/tables/numberData-1.csv")
val prime_num_rdd=prime_num.filter(line=>line!="Number")
prime_num_rdd.take(3)

// COMMAND ----------

def checkPrime(i :Int) : Boolean = {
     if (i <= 1)
       false
     else if (i == 2)
       true
     else
       !(2 to (i-1)).exists(x => i % x == 0)
   }

// COMMAND ----------

prime_num_rdd.map(x=>checkPrime(x.toInt)).countByValue()

// COMMAND ----------

val prime_num=prime_num_rdd.filter(x=>checkPrime(x.toInt))

// COMMAND ----------

val prime_num_Int=prime_num.map(x=>x.toInt)
prime_num_Int.take(3)

// COMMAND ----------

val prime_num_Int=prime_num.map(x=>x.toInt)
prime_num_Int.take(3)

// COMMAND ----------

val sum_prime=prime_num_Int.reduce((x,y)=>x+y)

// COMMAND ----------


