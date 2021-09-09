package sparkscala2.test

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.functions.col
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import com.databricks.spark.avro

object jsontodf2 {
  def main(args:Array[String]):Unit={
     val conf = new SparkConf().setAppName("first").setMaster("local[*]")
     val sc=new SparkContext(conf)
     sc.setLogLevel("ERROR")
     val spark=new SparkSession.Builder().getOrCreate()
  // val bankdata=spark.read.format("json").option("multiLine","true").load("file:///E://Zeyobron//DataSets//World_bank.json")
    val bankdata=spark.read.format("json").option("multiLine","true").load("file:///E://Zeyobron//DataSets//nyt2.json")
     bankdata.printSchema()
     bankdata.select("bestsellers_date.$date.$numberLong").show(false)
   //bankdata.withColumn("majorsector_percent", explode(col("majorsector_percent")))
   //.select("_id.$oid","majorsector_percent.Name","majorsector_percent.Percent").show(false)
}}