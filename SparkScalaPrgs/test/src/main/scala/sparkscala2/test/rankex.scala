package sparkscala2.test

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._

/*
 * sample.txt

customer_mob_number,recharge_amount,month(1to12)
1234,600,12
1234,400,2
2312,700,4
2312,500,5

*/

object rankex {
  def main(args:Array[String]):Unit= {

    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("SparkByExamples.com")
      .getOrCreate()
      var sparkConf: SparkConf = null

 sparkConf = new SparkConf().set("spark.sql.crossJoin.enabled", "true")
      spark.sparkContext.setLogLevel("ERROR")
  import spark.implicits._
  val df=spark.read.format("csv").option("header","true").load("file:///C://Users//User//Desktop//sample.txt")
  df.show()
  val flat_df=df.groupBy("customer_mob_number").agg(collect_list("recharge_amount") as "recharge_amount")
  flat_df.show()
  val flat_df1=flat_df.withColumn("recharge_amount_1",$"recharge_amount".getItem(0))
  .withColumn("recharge_amount_2",$"recharge_amount".getItem(1))
  .withColumn("sum",expr("recharge_amount_1 + recharge_amount_2")).drop("recharge_amount")
  flat_df1.show()
  }
}