package sparkscala2.test

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.functions.col
import org.apache.spark.sql._

object jsontodf1 {
  def main(args:Array[String]):Unit={
     val conf = new SparkConf().setAppName("first").setMaster("local[*]")
     val sc=new SparkContext(conf)
     sc.setLogLevel("ERROR")
     val spark=new SparkSession.Builder().getOrCreate()
     val df=spark.read.format("json").option("multiLine","true").load("file:///C://Users//User//Desktop//complex1.json")
     df.printSchema()
     df.show(false)
     df.withColumn("result",explode(col("required"))).
     select("$id","properties.city").show(false)
}}