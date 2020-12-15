// Databricks notebook source
var a =1


// COMMAND ----------

var a:Int =1
a = 10

// COMMAND ----------

var c =100

c=10

// COMMAND ----------

lazy val a =10
a

// COMMAND ----------

lazy val variable_lazy = {println("hello world");5}

variable_lazy

// COMMAND ----------

lazy val sum =10+b

val b=9

println(sum)

// COMMAND ----------

val `my name is sarbamon` = "kramsa" 

// COMMAND ----------

val `import` =10
print(`import`)


// COMMAND ----------

def square(a:Int):Int = {
  
  return a*a
  
}
square(9)

// COMMAND ----------

val a ="\t\n \u03bb"

// COMMAND ----------

def add(a:Int,b:Int) = a+b
print(add(6,7))

// COMMAND ----------

val `void` =100

val `false` =true

val `return` =90

if(`false`) `void` else `return`

// COMMAND ----------

def square(a:Int) :Int ={
  a*a
}

def sq2(y:Int, takefunctions:Int=>Int):Int={//take function here
  takefunctions(y) //square will be call
}

sq2(2,square)

// COMMAND ----------

var new2= Array(1,2,3,4,5)

// COMMAND ----------

new2(0)=100 
new2

// COMMAND ----------

val data =List(1,2,3,4,5,6,7)
//parallize method used to create rdd and is lazy in nature
val creationRDD =sc.parallelize(data)

// COMMAND ----------

//to get result of rdd you need to call action on rdd
creationRDD.collect()
//collect is an action operation use to revoke and remove the lazy.

// COMMAND ----------

//get the total partition for your data
creationRDD.partitions.size

// COMMAND ----------

val rddParttition = sc.parallelize(List(1,2,3),2)

// COMMAND ----------

rddParttition.partitions.size

// COMMAND ----------

rrdParttition.count()

// COMMAND ----------

rrdParttition.map(x => x*x*x )

// COMMAND ----------

maprdd.filter(x=> x%2 ==0).collect()

// COMMAND ----------

val mainrdd =sc.parallelize(List("hey","hello","sarbamon","sir"))
mainrdd.collect()

// COMMAND ----------

//map vs flatmap
mainrdd.map(x=>x.split(",")).collect()

// COMMAND ----------

mainrdd.flatMap(x=>x.split(",")).collect()

// COMMAND ----------

val keyrdd = rdd0.map(x=>(x,1))

keyrdd.collect()

// COMMAND ----------

val keyrdd = rdd0.map(x=>(x,1))

keyrdd.collect()

// COMMAND ----------


