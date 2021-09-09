package sparkscala2.test

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.functions.col
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import com.databricks.spark.avro

object jsontodf {
  def main(args:Array[String]):Unit={
     val conf = new SparkConf().setAppName("first").setMaster("local[*]")
     val sc=new SparkContext(conf)
     sc.setLogLevel("ERROR")
     val spark=new SparkSession.Builder().getOrCreate()
     val hrdata=spark.read.format("json").option("multiLine","true").load("file:///C://Users//User//Desktop//complex.json")
    hrdata.withColumn("employeeContributions", explode(col("employeeContributions")))
     .withColumn("employeeContributions1", explode(col("employeeContributions.employeecontributionsources")))
     .select("profileld","employeeContributions.employeeContributionId",
         "employeeContributions1.incomeType","employeeContributions1.displayorder",
          "employeeContributions1.annualIncrease.participantMaxType")
     .show(false)
     
  }
  
}