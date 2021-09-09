package sparkscala2.test

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.functions.regexp_extract


object example1 {
  //case class MyType(Field_Name: String)
 // case class click(rowtime:Map[String,String])
  case class click(rowtime:String,key:String,ip:String,userid:String,remote_user:String,time:String,_time:String,request:String,status:String,bytes:String,referrer:String,agent:String)
  def main(args:Array[String]):Unit={
   System.setProperty("hadoop.home.dir", "C:\\hadoop\\")
   val spark = SparkSession.builder().appName("test").master("local[*]").getOrCreate()
  	spark.sparkContext.setLogLevel("Error")
      import spark.implicits._
   val rdd = spark.sparkContext.textFile("file:///C://Users//User//Desktop//test1.txt")
    val clean_rdd=rdd.map(x=>x.replace("value: {","")).map(x=>x.replace("}","")).map(x=>x.replace("\"",""))
    
    
      val schema_rdd=clean_rdd.map(x=>x.split(",")).map(x=>click(x(0).split(":")(1),x(1).split(":")(1),x(2).split(":")(1),x(3).split(":")(1),x(4).split(":")(1),x(5).split(":")(1),
     x(6).split(":")(1),x(7).split(":")(1),x(8).split(":")(1),x(9).split(":")(1),x(10).split(":")(1),x(11).split(":")(1)))
 val final_df=schema_rdd.toDF()
 final_df.show(false)
  }
}

   
  
