package sparkscala2.test

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._
/*https://stackoverflow.com/questions/51850467/spark-scala-case-class-with-array-and-map-datatype
 * https://stackoverflow.com/questions/50444523/how-to-read-json-file-and-convert-to-case-class-with-spark-and-spray-json
 * https://stackoverflow.com/questions/62987950/spark-read-key-value-pairs-from-the-file-into-a-dataframe
 * 
 * Michael,100," Montreal,Toronto", 
 * Male,30, DB:80," Product,DeveloperLead" Will,101,
 *  Montreal, Male,35, Perl:85," Product,Lead,Test,Lead" Steven,102, 
 *  New York, Female,27, Python:80," Test,Lead,COE,Architect" Lucy,103, Vancouver, 
 *  Female,57, Sales:89_HR:94," Sales,Lead"
 */
object example2 {
  case class schema(name: String,employee_id:String ,work_place: Array[String],sex_age: Map [String,String],skills_score: Map[String,String],depart_title: Map[String,Array[String]])

def main(args:Array[String]):Unit= {
 System.setProperty("hadoop.home.dir", "C:\\hadoop\\")
   
    val conf=new SparkConf().setAppName("first_demo").setMaster("local[*]")
    val sc=new SparkContext(conf)
   val spark=SparkSession.builder().getOrCreate()
    import spark.implicits._
    val rdd1=sc.textFile("file:///C://Users//User//Desktop//example2.txt").map(x=>x.replace("[","")).map(x=>x.replace("]",""))
    //val clean_rdd=rdd1.map(x=>x.replace("[","")).map(x=>x.replace("]",""))
   val schema_rdd=rdd1.map(x=>x.split(", ")).map(x=>schema(x(0),x(1),x(2).split(","),Map(x(3).split(",")(0)->x(3).split(",")(1)),Map(x(4).split(":")(0)->x(4).split(":")(1)),Map(x(5).split(":")(0)->x(5).split(":"))))
    val df1=schema_rdd.toDF()
    df1.printSchema()
  df1.show(false)
}
}