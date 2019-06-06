package com.spark.structured.streaming

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.streaming.Trigger

object Producer {

  def kafkaProducer(outputDf: DataFrame, bootStrapServer: String, topic: String, triggerInterval: String = "5") =
    outputDf.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", bootStrapServer.mkString)
      .option("topic", topic)
      .trigger(Trigger.ProcessingTime(s"$triggerInterval seconds"))
      .start()

  def kafkaConsoleProducer(outputDf: DataFrame, triggerInterval: String = "3") =
    outputDf.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .writeStream
      .outputMode("complete")
      .format("console")
      .trigger(Trigger.ProcessingTime(s"$triggerInterval seconds"))
      .start()
}
