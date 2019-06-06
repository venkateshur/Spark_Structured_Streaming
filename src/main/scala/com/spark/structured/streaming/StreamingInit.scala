package com.spark.structured.streaming

import com.spark.structured.streaming.util.SparkSessionProvider
import org.apache.spark.sql.Encoders

case class CSVSchema(text: String)

object StreamingInit extends App with SparkSessionProvider {

  val getConfig = Config.getKafkaConfig(args(0))
  val parseConfig = Config.parseConfig(getConfig)
  val readData = parseConfig.inputSource match {
    case "csv" | "text" => Consumer.kafkaFileConsumer(spark, Encoders.product[CSVSchema].schema, "csv", args(1), Some(args(2)))
    case "kafka" => Consumer.kafkaConsumer(spark, parseConfig.bootStrapServers, parseConfig.consumerTopic)
  }

  val query = parseConfig.outputTarget match {
    case "console" => Producer.kafkaConsoleProducer(readData,parseConfig.triggerInterval)
    case "kafka"  => Producer.kafkaProducer(readData, parseConfig.bootStrapServers, parseConfig.producerTopic, parseConfig.triggerInterval)
  }
  query.awaitTermination
}
