package sparkscala2.test

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import java.net.URL
import org.apache.commons.io.IOUtils
import scala.io.Source
import org.apache.spark.sql.functions._

object rpadnull {
  def main(args:Array[String]):Unit= {

    val conf = new SparkConf().setAppName("ES").setMaster("local[*]")
      val sc = new SparkContext(conf)
 val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("readurldata")
      .getOrCreate()
 
      sc.setLogLevel("Error")
  import spark.implicits._
  val data = Seq((1,"2020−11−04 20:54:31"),(2,"null"),(3,"null"),(4,"2020−11−04 20:54:53"))
       
  val df = data.toDF("id","timestamp")
    //df.show()
    df.createOrReplaceTempView("df_view")
   // spark.sql("select rpad(nvl(timestamp,''),19,' ') from df_view").show()
//LPAD(‘Hello’,8,’ ‘) nvl(salary,-1)
    
    val df1 = Seq("10","20","59",null,null,"98","100","100","101","103","102").toDF("value")
    
    df1.select("value").filter(!$"value".between(98,102) or col("value").isNull).show()
    //df1.na.fill("0").where("Value<98 or value>102").show()//filter the data  not between 98 and 102
    
    
    
    /*+-----+
|value|
+-----+
|10   |
|20   |
|59   |
|null |
|null |
|98   |
|103  |
+-----+*/
    
  }
  
}