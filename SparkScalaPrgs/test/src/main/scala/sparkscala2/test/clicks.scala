package sparkscala2.test

//https://stackoverflow.com/questions/37335307/get-list-of-data-types-from-schema-in-apache-spark/54947031#54947031
//https://stackoverflow.com/questions/29369984/how-to-convert-keys-in-a-map-to-lower-case
//https://stackoverflow.com/questions/1822481/remove-characters-from-the-end-of-a-string-scala
//https://stackoverflow.com/questions/42537931/spark-hbase-connector-exception-java-lang-unsupportedoperationexception-empt
//spark-submit --conf spark.driver.extraClassPath="/home/cloudera/hbase/*" --jars "file:///home/cloudera/jarsforspark1/*" --class sparkscala2.test.clicks  --master local[*] sparkscala1-0.0.1-SNAPSHOT.jar
//sudo service hbase-master restart 
//sudo service hbase-regionserver restart

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.from_unixtime
import org.apache.spark.sql.functions.to_date
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.functions.unix_timestamp
import org.apache.spark.sql.execution.datasources.hbase._
object clicks {
  def main(args:Array[String]):Unit={
      System.setProperty("hadoop.home.dir", "C:\\hadoop\\")
      val spark = SparkSession.builder().appName("test").master("local[*]").getOrCreate()
  		spark.sparkContext.setLogLevel("Error")
      import spark.implicits._
      //spark.conf.set("parquet.enable.summary-metadata", "false")
      //spark.conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
     val clickstream=spark.read.format("json").option("multiLine","true").option("header","false").load("file:///D://clickstreamdata.json")
 //  val clickstream=spark.read.format("json").option("multiLine","true").option("header","false").load("file:///home/cloudera/clicks/clickstreamdata.json")
      clickstream.printSchema()
      
      val clickstream1=clickstream.withColumn("timestamp", from_unixtime($"timestamp","MM-dd-yyyy"))
      .select($"device_id".cast("string"),$"device_type".cast("string"),$"ip".cast("string"),$"signal".cast("string"),$"battery_level".cast("string"),$"c02_level".cast("string"),$"cca3".cast("string"),$"cn".cast("string"),$"temp".cast("string"),$"timestamp".cast("string"))
     clickstream1.printSchema()
      // val alldatatypes=clickstream1.dtypes.map(_._2.toLowerCase().dropRight(4)).toList
      val alldatatypes=List("string","string","string","string","string","string","string","string","string")
      val allcolumns=clickstream1.dtypes.map(_._1).drop(1).toList
      print(allcolumns)
      val a=allcolumns.length-1
      

      var str1=
      s"""{
        "table":{"namespace":"default","name":"clickstream_data3"},
        "rowkey":"device_id",
        "columns":{
        "device_id":{"cf":"rowkey","col":"device_id","type":"string"},\n"""
				
		    for(i<-0 to a)
		    {
		     if(i < a)
		     {
           var str = """""""+allcolumns(i)+s"""":{"cf":"cf","col":""""+allcolumns(i)+s"""","type":""""+alldatatypes(i)+s""""},\n"""
		       str1 = str1.concat(str)
		     }
		        else{
            var str = """""""+allcolumns(i)+s"""":{"cf":"cf","col":""""+allcolumns(i)+s"""","type":""""+alldatatypes(i)+s""""}"""
		       str1 = str1.concat(str)
		   }
  		}
 
       val catalog =  str1 +s""" 
          } 
          }"""
        print(catalog)

//append to the hbase table
      clickstream1.write.options(
      Map(HBaseTableCatalog.tableCatalog -> catalog, HBaseTableCatalog.newTable -> "4"))
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .save()
        
  }
  
}