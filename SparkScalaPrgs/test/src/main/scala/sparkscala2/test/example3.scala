package sparkscala2.test

//https://www.youtube.com/watch?v=sa-TUpSx1JA     ----  video for regex
/*Input data:
Ord_value|other_data
12345|u1=876435;u5=4356|4357|4358;u15=Mr. Noodles,n/a,Great Value;u16=0.77,4.92,7.96;u17=4,1,7;

Details of U variables
U1= orderId --single value
U5= pid --is a list
U15= name --is a list
U16= price -- is a list
U17= quantity -- is a list

Output:
Ord_value|orderid|pid|name|price|quantity
12345|876435|4356|Mr. Noodles|0.77|4
12345|876435|4357|n/a|4.92|1
* */
//https://stackoverflow.com/questions/42839726/spark-remove-special-characters-from-rows-dataframe-with-different-column-type

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.split
import org.apache.spark.sql.functions.regexp_replace

object example3 {
def main(args:Array[String]):Unit={
   System.setProperty("hadoop.home.dir", "C:\\hadoop\\")
   val conf=new SparkConf().setAppName("first_demo").setMaster("local[*]")
   val sc=new SparkContext(conf)
   val spark=SparkSession.builder().getOrCreate()
   spark.sparkContext.setLogLevel("Error")
   import spark.implicits._
//   val df1=spark.read.option("delimiter",";").textFile("file:///C://Users//User//Desktop//example3.txt")
//   //df1.show(false)
//      
//   val df2=df1.withColumn("value1",split($"value","\\|")).drop("value")//.withColumn("value2",regexp_replace(col("value1"),"[",""))
//   
// 
//   val splitDF = df2.withColumn("split_raw", split($"value1", ";"))
//  .withColumn("A", $"split_raw"(0))
//  .withColumn("B", $"split_raw"(1))
//  .withColumn("C", $"split_raw"(2))
//  .withColumn("D", $"split_raw"(3))
//splitDF.show(false)
   
  
   
   var df1 = spark.read.text("file:///C://Users//User//Desktop//example3.txt")
   
var df2 = (df1.withColumn("value", split($"value", "(?<=^[^|]*)\\|")).withColumn("OrderValue", $"value"(0)).withColumn("Rest", $"value"(1))).drop("value")

df2 = (df2.withColumn("Rest", split($"Rest", "(?<=^[^;]*)\\;")).withColumn("OrderId", $"Rest"(0)).withColumn("Rest1", $"Rest"(1))).drop("Rest")

df2 = (df2.withColumn("OrderIdd",regexp_replace(df2("OrderId"),"u1=",""))).drop("OrderId")

df2 = (df2.withColumn("Rest1", split($"Rest1", ";")).withColumn("PId", $"Rest1"(0)).withColumn("Name", $"Rest1"(1)).withColumn("Price", $"Rest1"(2)).withColumn("Quntity", $"Rest1"(3))).drop("Rest1")
var df3=df2
df3=df3.withColumn("PId",split($"PId","(?<=^[^=]*)\\=")).withColumn("PId",regexp_replace($"PId"(1),"\\|",",")).withColumn("PId",split($"PId",","))
df3=df3.withColumn("Name",split($"Name","(?<=^[^=]*)\\=")).withColumn("Name",regexp_replace($"Name"(1),"\\|",",")).withColumn("Name",split($"Name",","))
df3=df3.withColumn("Price",split($"Price","(?<=^[^=]*)\\=")).withColumn("Price",regexp_replace($"Price"(1),"\\|",",")).withColumn("Price",split($"Price",","))
df3=df3.withColumn("Quntity",split($"Quntity","(?<=^[^=]*)\\=")).withColumn("Quntity",regexp_replace($"Quntity"(1),"\\|",",")).withColumn("Quntity",split($"Quntity",","))
df3.show(false)
var joined_df = (df3.select($"OrderValue",$"OrderIdd",posexplode($"PId")).withColumnRenamed("col","PId").alias("a")
.join(df3.select($"OrderIdd",posexplode($"Name")).withColumnRenamed("col","Name").alias("b"),col("a.OrderIdd") === col("b.OrderIdd") && col("a.pos") === col("a.pos"), "inner")
.join(df3.select($"OrderIdd",posexplode($"Price")).withColumnRenamed("col","Price").alias("c"),col("a.OrderIdd") === col("c.OrderIdd") && col("a.pos") === col("c.pos"), "inner")
.join(df3.select($"OrderIdd",posexplode($"Quntity")).withColumnRenamed("col","Quantity").alias("d"),col("a.OrderIdd") === col("d.OrderIdd") && col("a.pos") === col("d.pos"), "inner")
).drop("pos").select(col("a.OrderValue"),col("a.OrderIdd").as("OrderId"),col("b.Name"),col("c.Price"),col("d.Quantity"),col("a.PId"))
joined_df.show()
     
     }
}