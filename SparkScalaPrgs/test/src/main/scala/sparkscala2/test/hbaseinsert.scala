package sparkscala2.test

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.from_unixtime
import org.apache.spark.sql.functions.to_date
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.functions.unix_timestamp
import org.apache.spark.sql.execution.datasources.hbase._
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog

object hbaseinsert {
  
 case class Employee(key: String, fName: String, lName: String, mName: String,
                      addressLine: String, city: String, state: String, zipCode: String)
   def main(args:Array[String]):Unit={
   System.setProperty("hadoop.home.dir", "C:\\hadoop\\")
   
  
  // as pre-requisites the table 'employee' with column families 'person' and 'address' should exist
//  val tableNameString = "default:employee"
//  val colFamilyPString = "person"
//  val colFamilyAString = "address"
//  val tableName = TableName.valueOf(tableNameString)
//  val colFamilyP = colFamilyPString.getBytes
//  val colFamilyA = colFamilyAString.getBytes

//  val hBaseConf = HBaseConfiguration.create()
//  val connection = ConnectionFactory.createConnection(hBaseConf);
//  val admin = connection.getAdmin();

//  println("Check if table 'employee' exists:")
//  val tableExistsCheck: Boolean = admin.tableExists(tableName)
//  println(s"Table " + tableName.toString + " exists? " + tableExistsCheck)

//  if(tableExistsCheck == false) {
//    println("Create Table employee with column families 'person' and 'address'")
//    val colFamilyBuild1 = ColumnFamilyDescriptorBuilder.newBuilder(colFamilyP).build()
//    val colFamilyBuild2 = ColumnFamilyDescriptorBuilder.newBuilder(colFamilyA).build()
//    val tableDescriptorBuild = TableDescriptorBuilder.newBuilder(tableName)
//      .setColumnFamily(colFamilyBuild1)
//      .setColumnFamily(colFamilyBuild2)
//      .build()
//    admin.createTable(tableDescriptorBuild)
//  }
  
   // define schema for the dataframe that should be loaded into HBase
  def catalog =
    s"""{
       |"table":{"namespace":"default","name":"employee"},
       |"rowkey":"key",
       |"columns":{
       |"key":{"cf":"rowkey","col":"key","type":"string"},
       |"fName":{"cf":"person","col":"firstName","type":"string"},
       |"lName":{"cf":"person","col":"lastName","type":"string"},
       |"mName":{"cf":"person","col":"middleName","type":"string"},
       |"addressLine":{"cf":"address","col":"addressLine","type":"string"},
       |"city":{"cf":"address","col":"city","type":"string"},
       |"state":{"cf":"address","col":"state","type":"string"},
       |"zipCode":{"cf":"address","col":"zipCode","type":"string"}
       |}
       |}""".stripMargin
       
       print(catalog)
       
       // define some test data
  val data = Seq(
    Employee("1","Horst","Hans","A","12main","NYC","NY","123"),
    Employee("2","Joe","Bill","B","1337ave","LA","CA","456"),
    Employee("3","Mohammed","Mohammed","C","1Apple","SanFran","CA","678")
  )

  // create SparkSession
  val spark: SparkSession = SparkSession.builder()
    .master("local[*]")
    .appName("HBaseConnector")
    .getOrCreate()

  // serialize data
  import spark.implicits._
  val df = spark.sparkContext.parallelize(data).toDF
  df.printSchema()
  
  // write dataframe into HBase
//  df.write.options(
//    Map(HBaseTableCatalog.tableCatalog -> catalog, HBaseTableCatalog.newTable -> "4")) // create 3 regions
//    .format("org.apache.spark.sql.execution.datasources.hbase")
//    .save()
 }
}