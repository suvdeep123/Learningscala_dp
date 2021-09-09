package sparkscala2.test

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;

object wordcount {
  def main(args:Array[String]):Unit={
     val conf = new SparkConf().setAppName("first").setMaster("local[*]")
     val sc=new SparkContext(conf)
     sc.setLogLevel("ERROR")
     val input=sc.textFile("file:///E://Zeyobron//DataSets//wordcount.txt")
     val count=input.flatMap(line=>line.split(" ")).
     map(word=>(word,1)).reduceByKey(_+_)
     count.saveAsTextFile("file:///E://Zeyobron//DataSets//outputofwordcount")
     println("done")  
  }
}