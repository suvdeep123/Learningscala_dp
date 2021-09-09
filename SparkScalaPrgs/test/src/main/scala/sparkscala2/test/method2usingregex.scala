package sparkscala2.test
//https://kontext.tech/column/code-snippets/338/read-json-as-data-frame-spark-scala

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
//import org.apache.spark.sql.expressions._
import org.apache.spark.sql.types._

/*case class nested(ip: String, userid: String, remote_user: String, time: String, _time: String, request: String, status: String, bytes: String, referrer: String, agent: String)
import org.apache.spark.sql.Encoders
val schema = Encoders.product[nested].schema
Now pass this schema variable to from_json. you will get same answer.*/
  
object method2usingregex 
{
  def main(args:Array[String]):Unit= {
 System.setProperty("hadoop.home.dir", "C:\\hadoop\\")
 val spark = SparkSession.builder().master("local[*]").getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")
  import spark.implicits._
 
  // Assuming you have fixed structure. You can improvise this regex as required
  val regex = "([a-z-A-Z]+:\\s+)([\\s+\\d:\\./]+Z)([,\\s+a-z-A-Z]+:\\s+)([\\d\\.]+)([,\\s+]+[a-z-A-Z]+:)(.*)"
  /*
  ([a-z-A-Z]+:\\s+) --> Matches rowtime:
  ([\\s+\\d:\\./]+Z)  --> Matches rowtime value eg. 2020/06/11 10:38:42.449 Z
  ([,\\s+a-z-A-Z]+:\\s+) --> Matches , key:
  ([\\d\\.]+)  --> Matches content of key e.g 222.90.225.227
  ([,\\s+]+[a-z-A-Z]+:)  --> Matches , value:
  (.*) --> Matches content of value field which is in json
   */

  // Read file as dataframe and using regex and grouping ID extract column content
  var df = spark.read.textFile("file:///C://Users//User//Desktop//test1.txt")
    .select(regexp_extract('value, regex, 2).as("rowtime"),
      regexp_extract('value, regex, 4).as("key"),
      regexp_extract('value, regex, 6).as("value"))

  // Since value is json we can make use from_json to create struct field. "schema_of_json" this function is in spark 2.4.0 onwards
  df = df.withColumn("value", from_json('value, schema_of_json(df.select("value").first().getString(0))))

  // select all the column including nested columns of value column
  df.select("rowtime", "key", "value.*").show(false)
}
}