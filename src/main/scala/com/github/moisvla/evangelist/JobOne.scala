package com.github.moisvla.evangelist

import org.apache.spark.sql.{Dataset, Encoders}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.functions._

object JobOne
  extends App
    with SparkSupport
    with ArgumentResolver {

  import sparkSession.implicits._

  val (inputJsonPath, outputCsvPath) = resolveArgs(args)
  logger.info(s"Input JSON Path: $inputJsonPath")
  logger.info(s"Output CSV Path: $outputCsvPath")

  val extractedJSON: String = firstLine(inputJsonPath).getOrElse(sys.exit)

  val rsvp = buildFrame(extractedJSON)

  val patternToOmit = "[^'A-Za-zÀ-ÖØ-öø-ÿ0-9_ \\-]"
  val patternMultipleSpaces = " +"
  val patternSpace = " "

  // parse the json in csv friendly format
  // replace characters which are not letters / digits / accents / & / - / ' with a space
  // replace multiple spaces with just one space
  val writableDataset: Dataset[CSV] = rsvp
    .withColumn("event_name", regexp_replace($"event.event_name", patternToOmit, patternSpace))
    .withColumn("event_name", regexp_replace($"event_name", patternMultipleSpaces, patternSpace))
    .withColumn("group_topic", explode($"group.group_topics.topic_name"))
    .withColumn("group_topic", regexp_replace($"group_topic", patternToOmit, patternSpace))
    .withColumn("group_topic", regexp_replace($"group_topic", patternMultipleSpaces, patternSpace))
    .withColumn("group_name", regexp_replace($"group.group_name", patternToOmit, patternSpace))
    .withColumn("group_name", regexp_replace($"group_name", patternMultipleSpaces, patternSpace))
    .select("group_topic", "event_name", "group_name", "rsvp_id")
    .as[CSV]

  try writeToCsv(writableDataset.toDF, outputCsvPath)
  finally removeFirstLineFromFile(inputJsonPath)

  def buildFrame(json: String): Dataset[RSVP] = {
    val schema: StructType = Encoders.product[RSVP].schema
    sparkSession.read.schema(schema).json(Seq(json).toDS).as[RSVP]
  }

}
