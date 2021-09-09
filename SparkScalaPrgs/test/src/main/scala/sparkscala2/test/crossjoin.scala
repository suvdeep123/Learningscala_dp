//Will give you some hints. You could use transform function to manipulate array column. But that function is available from spark V2.4.0. For 2.3.1 you have to come up with UDF.
//https://chat.stackoverflow.com/rooms/236266/discussion-between-mohana-b-c-and-anonymous
//https://medium.com/analytics-vidhya/intro-to-window-function-in-pyspark-with-examples-3a839b6e1cac

package sparkscala2.test

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._

object crossjoin {
  def main(args:Array[String]):Unit= {

    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("SparkByExamples.com")
      .getOrCreate()
      var sparkConf: SparkConf = null

 //sparkConf = new SparkConf().set("spark.sql.crossJoin.enabled", "true")
      spark.sparkContext.setLogLevel("ERROR")
  import spark.implicits._
 
   val df1 = List("IN","PK", "AU","SL").toDF("country")
  // Combinations column will have array[country], countries will be picked up from next rows
  df1.withColumn("combinations", collect_set("country").
      over(Window.rowsBetween(Window.currentRow + 1, Window.unboundedFollowing)))
    // Last row will have empty array, filter that
   .where(size('combinations) > 0)
    // Concat each element of array column with country
   .withColumn("combinations",expr("transform(combinations, c-> concat_ws(' vs ', country, c))"))
    // Explode array to get each element of array in rows.
   .select(explode('combinations))
   .show(false)
   
}
}