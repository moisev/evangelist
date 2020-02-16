package com.github.moisvla.evangelist

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset, Encoders}
import org.apache.spark.sql.functions._

object JobTwo extends App
  with SparkSupport
  with ArgumentResolver {

  import sparkSession.implicits._
  val (inputCSVPath, outputPath) = resolveArgs(args)
  val csvDF = readCsv.cache()

  val distinctNameWords = extractUniqueNameWords(csvDF)
  val groupTopics = extractTopicWords(csvDF)
  val hotTopics = unionAndExtractTopN(distinctNameWords, groupTopics, 5)

  writeToCsv(hotTopics.toDF, outputPath, " :: ", false)

  def readCsv: Dataset[CSV] = {
    val schema: StructType = Encoders.product[CSV].schema
    sparkSession
      .read
      .format("csv")
      .option("header", "false")
      .schema(schema)
      .load(inputCSVPath)
      .as[CSV]
  }

  def extractUniqueNameWords(ds: Dataset[CSV]): DataFrame = {
    ds.select("rsvp_id", "group_name", "event_name")
      .distinct()
      .select(split(concat(lower($"group_name"), lit(" "), lower($"event_name"))," ").as("all_words"))
      .withColumn("all_words", explode($"all_words"))
      .filter(length($"all_words")>3)
  }

  def extractTopicWords(ds: Dataset[CSV]): DataFrame = {
    ds.select(split(lower($"group_topic")," ").as("all_words"))
      .withColumn("all_words", explode($"all_words"))
      .filter(length($"all_words")>3)
  }

  def unionAndExtractTopN(df1: DataFrame, df2: DataFrame, n: Int): DataFrame = {
    df1.unionByName(df2)
      .groupBy("all_words").count()
      .orderBy(desc("count"))
      .withColumn("current_timestamp", date_format(current_timestamp(), "yyyy-MM-dd hh:mm:ss"))
      .select("current_timestamp", "all_words", "count")
      .limit(n)
  }

}
