package com.spark.structured.streaming

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.json4s._
import org.json4s.jackson.JsonMethods._

import java.util.UUID

object Processor {

  implicit val formats: DefaultFormats.type = DefaultFormats // For JSON parsing

  def apply(inDf: DataFrame, tableName: String, joinCondition: String)(implicit spark: SparkSession): DataFrame = {
    // Collect and parse the JSON payload
    val payLoad = inDf.collect().flatMap(row => {
      val jsonString = row.getString(0) // Assuming the first column contains the JSON string
      try {
        Some(parse(jsonString).extract[Map[String, String]]) // Parse JSON into a Map
      } catch {
        case _: Exception =>
          None // Ignore invalid JSON strings
      }
    })

    if (payLoad.isEmpty) {
      throw new IllegalArgumentException("No valid JSON payloads found in input DataFrame.")
    }

    // Extract fields and values from the payload
    val extractedFields = payLoad.flatMap(_.keys).distinct
    val extractedValues = payLoad.map(_.values)

    // Construct a condition or query dynamically based on fields
    val hiveQuery = s"SELECT * FROM $tableName WHERE ${buildWhereClauseFromPayload(payLoad)}"

    // Execute the query
    val hiveDf = spark.sql(hiveQuery)

    val generateUUID = udf(() => UUID.randomUUID().toString)
    val jsonDf = hiveDf.withColumn("value", to_json(struct(hiveDf.columns.map(col): _*)))
      .withColumn("key", generateUUID())

    jsonDf
  }

  private def buildWhereClauseFromPayload(payload: Array[Map[String, String]]): String = {
    // Builds a WHERE clause based on the first map for simplicity
    // This logic can be enhanced to handle more complex scenarios
    payload.headOption.map { map =>
      map.map {
        case (key, value) =>
          val safeValue = value.replace("'", "\\'") // Escape single quotes for safety
          s"$key = '$safeValue'"
      }.mkString(" AND ")
    }.getOrElse("1 = 1") // Fallback condition if no payload is present
  }
}
