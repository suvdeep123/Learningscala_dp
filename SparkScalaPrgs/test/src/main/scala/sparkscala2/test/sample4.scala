package sparkscala2.test

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.window
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._


object sample4 {
  
  def main(args: Array[String]): Unit = {
//    val spark = SparkSession.builder()
//      .appName("Test")
//      .master("local[*]")
//      .getOrCreate()
//    import spark.implicits._
//    val df = Seq(("1","1","1"),("2","2","2"),("3","3","3"),("4","4",null),("5","5","5"),("6","6","6"),("7","7","7"),("8","8",null),("9","9","9")).toDF("c1","c2","c3")
//    df.show()
//    df.withColumn("previous_val", lag("c3", 1, 0).over(Window.partitionBy().orderBy(col("c1"),col("c2"))))
//      .select("*").filter(col("c3").isNull || col("previous_val").isNull)
//      .drop(col("previous_val"))
//      .show
//    spark.stop()
//  }
    

			val conf = new SparkConf().setAppName("ES").setMaster("local[*]")
					val sc = new SparkContext(conf)
					sc.setLogLevel("ERROR")

					val spark = SparkSession.builder().getOrCreate()
					import spark.implicits._

					val df = Seq(("1","1","1"),("2","2","2"),("3","3","3"),("4","4",null),("5","5","5"),("6","6","6"),("7","7","7"),("8","8",null),("9","9","9")).toDF("c1","c2","c3") 
					val windowSpec = Window.orderBy('C2)

					val lagCol = lag(col("C3"), 1,0).over(windowSpec)
					
					val newdf= df.withColumn("LagCol", lagCol)
					newdf.show()
					val resultdf=newdf.filter(col("c3").isNull or col("LagCol").isNull)//.drop("LagCol")
					resultdf.show()		


	}


}