package com.spark.structured.streaming

import com.spark.structured.streaming.util.SparkSessionProvider
import com.typesafe.config.{ConfigFactory, _}

import scala.util.{Failure, Success, Try}

case class KafkaConfig(bootStrapServers: String, consumerTopic: String, producerTopic: String, inputSource: String, outputTarget: String, triggerInterval: String)

object Config extends SparkSessionProvider{

  def getKafkaConfig(configFile: String): Config = Try(ConfigFactory.parseResources(configFile)) match {
    case Success(value) => value
    case Failure(e)  => throw e
  }

  def parseConfig(parseConf: Config): KafkaConfig = try {
    KafkaConfig(parseConf.getString("kafkaConfig.bootStrapServers"),
      parseConf.getString("kafkaConfig.consumerTopic"),
      parseConf.getString("kafkaConfig.producerTopic"),
      parseConf.getString("kafkaConfig.inputSource"),
      parseConf.getString("kafkaConfig.outputTarget"),
      parseConf.getString("kafkaConfig.triggerInterval")
    )
  } catch {
    case e: Exception => throw e
  }
}
