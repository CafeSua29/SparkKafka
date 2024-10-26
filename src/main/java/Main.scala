import Main.spark
import hbase.HBaseConnectionFactory
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.functions._

import java.sql.Timestamp
import java.time.LocalDateTime
import java.util

object Main {
  val spark = SparkSession.builder().getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  // Method to create DataFrame from log file
  def createLogDataFrame(spark: SparkSession, logFilePath: String): DataFrame = {
    val schema = StructType(Array(
      StructField("timeCreate", TimestampType, true),
      StructField("cookieCreate", TimestampType, true),
      StructField("browserCode", IntegerType, true),
      StructField("browserVer", StringType, true),
      StructField("osCode", IntegerType, true),
      StructField("osVer", StringType, true),
      StructField("ip", LongType, true),
      StructField("locId", IntegerType, true),
      StructField("domain", StringType, true),
      StructField("siteId", IntegerType, true),
      StructField("cId", IntegerType, true),
      StructField("path", StringType, true),
      StructField("referer", StringType, true),
      StructField("guid", LongType, true),
      StructField("flashVersion", StringType, true),
      StructField("jre", StringType, true),
      StructField("sr", StringType, true),
      StructField("sc", StringType, true),
      StructField("geographic", IntegerType, true),
      StructField("field19", StringType, true),
      StructField("field20", StringType, true),
      StructField("url", StringType, true),
      StructField("field22", StringType, true),
      StructField("category", StringType, true),
      StructField("field24", StringType, true)
    ))

    spark.read
      .option("delimiter", "\t")
      .option("header", "false")
      .schema(schema)
      .csv(logFilePath)
  }

  // Method to split DataFrame into batches and send to Kafka
  def splitAndSendToKafka(logData: DataFrame, kafkaTopic: String, kafkaServers: String): Unit = {
    val batchDFs = logData.randomSplit(Array.fill(logData.count().toInt / 100)(1.0), seed = 123L)

    batchDFs.foreach(df => {
      df.write
        .format("kafka")
        .option("kafka.bootstrap.servers", kafkaServers)
        .option("topic", kafkaTopic)
        .save()

      Thread.sleep(10000)  // Wait for 10 seconds
    })
  }

  // Method to read from Kafka as streaming DataFrame
//  def readFromKafka(spark: SparkSession, kafkaServers: String, kafkaTopic: String): DataFrame = {
//    spark.readStream
//      .format("kafka")
//      .option("kafka.bootstrap.servers", kafkaServers)
//      .option("subscribe", kafkaTopic)
//      .load()
//      .selectExpr("CAST(value AS STRING) as message")  // Keep as DataFrame, message field holds log string
//  }
//
//  def parseLogData(logStream: DataFrame): DataFrame = {
//    logStream.selectExpr("split(message, '\t') as fields") // Split log message by tab
//      .select(
//        col("fields").getItem(0).cast("timestamp").as("timeCreate"),
//        col("fields").getItem(1).cast("timestamp").as("cookieCreate"),
//        col("fields").getItem(2).cast("int").as("browserCode"),
//        col("fields").getItem(3).as("browserVer"),
//        col("fields").getItem(4).cast("int").as("osCode"),
//        when(col("fields").getItem(5).isNotNull, col("fields").getItem(5)).otherwise(lit("-1")).as("osVer"),
//        col("fields").getItem(6).cast("long").as("ip"),
//        col("fields").getItem(7).cast("int").as("locId"),
//        col("fields").getItem(8).as("domain"),
//        col("fields").getItem(9).cast("int").as("siteId"),
//        col("fields").getItem(10).cast("int").as("cId"),
//        col("fields").getItem(11).as("path"),
//        col("fields").getItem(12).as("referer"),
//        col("fields").getItem(13).cast("long").as("guid"),
//        col("fields").getItem(14).as("flashVersion"),
//        col("fields").getItem(15).as("jre"),
//        col("fields").getItem(16).as("sr"),
//        col("fields").getItem(17).as("sc"),
//        col("fields").getItem(18).cast("int").as("geographic"),
//        col("fields").getItem(19).as("field19"),
//        col("fields").getItem(20).as("field20"),
//        col("fields").getItem(21).as("url"),
//        col("fields").getItem(22).as("field22"),
//        col("fields").getItem(23).as("category"),
//        col("fields").getItem(24).as("field24")
//      )
//  }
//
//  // Method to write logs to HDFS by day
//  def writeLogsToHDFS(logStream: DataFrame, hdfsBasePath: String): Unit = {
//    val parsedDF = parseLogData(logStream)
//
//    parsedDF.writeStream
//      .format("parquet")
//      .option("path", hdfsBasePath)
//      .option("checkpointLocation", hdfsBasePath + "/checkpoints")
//      .partitionBy("timeCreate")
//      .start()
//  }
//
//  // Method to generate real-time reports
//  def generateRealTimeReports(logStream: DataFrame): Unit = {
//    val parsedDF = parseLogData(logStream)
//
//    val windowedCounts = parsedDF
//      .withWatermark("timeCreate", "10 minutes")
//      .groupBy(window(col("timeCreate"), "10 minutes"), col("domain"))
//      .agg(count("domain").alias("view_count"))
//      .orderBy(desc("view_count"))
//
//    windowedCounts.writeStream
//      .outputMode("complete")
//      .format("console")
//      .start()
//  }
//
//  // Method to write log data to HBase
//  def writeToHBase(logStream: DataFrame, tableName: String): Unit = {
//    var df = parseLogData(logStream)
//
//    df.printSchema()
//
//    df = df
//      .withColumn("day", date_format(col("timeCreate"), "yyyy-MM-dd"))
//      .repartition(5)
//
//    val batchPutSize = 100
//
//    df.foreachPartition((rows: Iterator[Row]) => {
//      val hbaseConnection = HBaseConnectionFactory.createConnection()
//      //val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
//
//      try {
//        val table = hbaseConnection.getTable(TableName.valueOf(tableName))
//        val puts = new util.ArrayList[Put]()
//        for (row <- rows) {
//          val timeCreate = row.getAs[Timestamp]("timeCreate").toString
//          val cookieCreate = row.getAs[Timestamp]("cookieCreate").toString
//          val browserCode = row.getAs[Int]("browserCode")
//          val browserVer = row.getAs[String]("browserVer")
//          val osCode = row.getAs[Int]("osCode")
//          var osVer = row.getAs[String]("osVer")
//
//          if (osVer == null)
//            osVer = "-1"
//
//          val ip = row.getAs[Long]("ip")
//          val locId = row.getAs[Int]("locId")
//          val domain = row.getAs[String]("domain")
//          val siteId = row.getAs[Int]("siteId")
//          val cId = row.getAs[Int]("cId")
//          val path = row.getAs[String]("path")
//          val referer = row.getAs[String]("referer")
//          val guid = row.getAs[Long]("guid")
//          val flashVersion = row.getAs[String]("flashVersion")
//          val jre = row.getAs[String]("jre")
//          val sr = row.getAs[String]("sr")
//          val sc = row.getAs[String]("sc")
//          val geographic = row.getAs[Int]("geographic")
//          val url = row.getAs[String]("url")
//          val category = row.getAs[String]("category")
//          val day = row.getAs[String]("day")
//
//          val put = new Put(Bytes.toBytes(guid))
//          put.addColumn(Bytes.toBytes("consumer"), Bytes.toBytes("timeCreate"), Bytes.toBytes(timeCreate))
//          put.addColumn(Bytes.toBytes("consumer"), Bytes.toBytes("cookieCreate"), Bytes.toBytes(cookieCreate))
//          put.addColumn(Bytes.toBytes("hardware"), Bytes.toBytes("browserCode"), Bytes.toBytes(browserCode))
//          put.addColumn(Bytes.toBytes("hardware"), Bytes.toBytes("browserVer"), Bytes.toBytes(browserVer))
//          put.addColumn(Bytes.toBytes("hardware"), Bytes.toBytes("osCode"), Bytes.toBytes(osCode))
//          put.addColumn(Bytes.toBytes("hardware"), Bytes.toBytes("osVer"), Bytes.toBytes(osVer))
//          put.addColumn(Bytes.toBytes("consumer"), Bytes.toBytes("ip"), Bytes.toBytes(ip))
//          put.addColumn(Bytes.toBytes("consumer"), Bytes.toBytes("locId"), Bytes.toBytes(locId))
//          put.addColumn(Bytes.toBytes("producer"), Bytes.toBytes("domain"), Bytes.toBytes(domain))
//          put.addColumn(Bytes.toBytes("producer"), Bytes.toBytes("siteId"), Bytes.toBytes(siteId))
//          put.addColumn(Bytes.toBytes("consumer"), Bytes.toBytes("cId"), Bytes.toBytes(cId))
//          put.addColumn(Bytes.toBytes("producer"), Bytes.toBytes("path"), Bytes.toBytes(path))
//          put.addColumn(Bytes.toBytes("producer"), Bytes.toBytes("referer"), Bytes.toBytes(referer))
//          put.addColumn(Bytes.toBytes("consumer"), Bytes.toBytes("guid"), Bytes.toBytes(guid))
//          put.addColumn(Bytes.toBytes("hardware"), Bytes.toBytes("flashVersion"), Bytes.toBytes(flashVersion))
//          put.addColumn(Bytes.toBytes("hardware"), Bytes.toBytes("jre"), Bytes.toBytes(jre))
//          put.addColumn(Bytes.toBytes("hardware"), Bytes.toBytes("sr"), Bytes.toBytes(sr))
//          put.addColumn(Bytes.toBytes("hardware"), Bytes.toBytes("sc"), Bytes.toBytes(sc))
//          put.addColumn(Bytes.toBytes("consumer"), Bytes.toBytes("geographic"), Bytes.toBytes(geographic))
//          put.addColumn(Bytes.toBytes("producer"), Bytes.toBytes("url"), Bytes.toBytes(url))
//          put.addColumn(Bytes.toBytes("producer"), Bytes.toBytes("category"), Bytes.toBytes(category))
//          put.addColumn(Bytes.toBytes("consumer"), Bytes.toBytes("day"), Bytes.toBytes(day))
//
//          puts.add(put)
//          if (puts.size > batchPutSize) {
//            table.put(puts)
//            puts.clear()
//          }
//        }
//        if (puts.size() > 0) {
//          table.put(puts)
//        }
//      } finally {
//        hbaseConnection.close()
//        println("done")
//      }
//    })
//  }

  def main(args: Array[String]): Unit = {
    val logFilePath = "sample text"
    val kafkaTopic = "pageviewlog"
    val kafkaServers = "localhost:9092"
    val hbaseTable = "pageviewlog2"
    val hdfsBasePath = "hdfs://namenode:9000/user/test/pageviewlog"

    // 1. Create DataFrame from Log File
    val logData = createLogDataFrame(spark, logFilePath)

    logData.show()

    // 2. Split DataFrame and Send to Kafka
    splitAndSendToKafka(logData, kafkaTopic, kafkaServers)

    // 3. Spark Streaming: Read from Kafka
//    val kafkaStream = readFromKafka(spark, kafkaServers, kafkaTopic)
//
//    kafkaStream.show()
//
//    // 4. Write logs to HDFS by day
//    writeLogsToHDFS(kafkaStream, hdfsBasePath)
//
//    // 5. Real-Time Reporting
//    generateRealTimeReports(kafkaStream)
//
//    // 6. Write access history to HBase
//    writeToHBase(kafkaStream, hbaseTable)

    spark.streams.awaitAnyTermination()
  }
}
