package sparkscala2.test
//https://stackoverflow.com/questions/52521043/get-results-from-url-using-scala-spark

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import java.net.URL
import org.apache.commons.io.IOUtils
import scala.io.Source
import org.apache.spark.sql.functions._

object getdatafromurl {
  def main(args:Array[String]):Unit= {

    val conf = new SparkConf().setAppName("ES").setMaster("local[*]")
      val sc = new SparkContext(conf)
 val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("readurldata")
      .getOrCreate()
 
      sc.setLogLevel("Error")
  import spark.implicits._
 System.setProperty("http.agent", "Chrome")
  /*
  def GetUrlContentJson(url: String): DataFrame ={
    val result = scala.io.Source.fromURL(url).mkString
    //only one line inputs are accepted. (I tested it with a complex Json and it worked)
    val jsonResponseOneLine = result.toString().stripLineEnd 
    //You need an RDD to read it with spark.read.json! This took me some time. However it seems obvious now 
    val jsonRdd = spark.sparkContext.parallelize(jsonResponseOneLine :: Nil) 

    val jsonDf = spark.read.json(jsonRdd)
    return jsonDf
}  
val response = GetUrlContentJson("https://stream.meetup.com/2/rsvps")
response.show
* */
//reading the web api url
  val userdata = scala.io.Source.fromURL("https://stream.meetup.com/2/rsvps/?results=10").mkString
  System.setProperty("http.agent", "Chrome")
 
  val jsonResponseOneLine = userdata.toString().stripLineEnd   
  val json = sc.parallelize(userdata :: Nil)
    val json_file = spark.read.option("multiLine", "true").json(json)
    json_file.printSchema()
    
    
  }
}