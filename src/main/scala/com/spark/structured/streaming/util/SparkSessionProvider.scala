package com.spark.structured.streaming.util

import org.apache.spark.sql.SparkSession

trait SparkSessionProvider {

  implicit val spark = SparkSession
    .builder()
    .appName("kafka spark structured streaming")
    .enableHiveSupport()
    .getOrCreate()

}
