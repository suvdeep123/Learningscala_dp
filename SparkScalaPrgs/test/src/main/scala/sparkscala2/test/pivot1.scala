
package sparkscala2.test

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object pivot1 {
  def main(args:Array[String]):Unit= {

    val spark: SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("SparkByExamples.com")
      .getOrCreate()

    val data = Seq(("amazon",100),("flipkart",100),("ebay",890),("amazon",100),
        ("flipkart",100),("ebay",10),("amazon",100),("flipkart",100),("ebay",10))



    import spark.sqlContext.implicits._
    val df = data.toDF("company","sales")
    df.show()

    //pivot --- this is not working
//    val pivotDF = df     
//      .groupBy("company")
//      .pivot("company")
//      .sum("sales")
      
      df.groupBy(lit(1))
.pivot("company")
.agg(sum("sales")).drop("1")
.show()




//this is working
//df .groupBy()
//.pivot("company")
//.agg(sum("sales"))
//.show()
    
    //pivotDF.show()
  }
}