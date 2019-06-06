package com.spark.structured.streaming

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.StructType

object Consumer {

  def kafkaConsumer(spark: SparkSession, bootSrapServer: String, topic: String): DataFrame = {
    import spark.implicits._
    val consumer = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
      .option("subscribe", "topic1")
      .load()
    consumer.select($"value")
  }

  def kafkaFileConsumer(spark: SparkSession, schema: StructType, inputPath: String, format: String, csvSep: Option[String]): DataFrame = {
    import spark.implicits._
    val consumer = format match {
      case "csv" => spark.readStream.option("sep", csvSep.getOrElse(";")).schema(schema).csv(inputPath)
      case "text" => spark.readStream.text(inputPath)
    }
    consumer.select($"value")
  }
}
