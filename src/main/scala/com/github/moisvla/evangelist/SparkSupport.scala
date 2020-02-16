package com.github.moisvla.evangelist

import java.io.{BufferedWriter, File, FileWriter}
import java.nio.file.{Files, Paths}

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.io.Source

trait SparkSupport {

  val path: String = Paths.get ("").toAbsolutePath.toString + "/spark-warehouse"
  implicit lazy val sparkSession: SparkSession = SparkSession
    .builder()
    .config("spark.sql.warehouse.dir", path)
    .appName(getClass.getName)
    .master("local[*]")
    .getOrCreate()

  lazy val sc: SparkContext = sparkSession.sparkContext
  sc.setLogLevel("ERROR")

  /*
  Make sure the job doesn't fail when upstream data is missing
  Close the source when done
  */
  def firstLine(file: String): Option[String] = {
    val src = Source.fromFile(file)("UTF-8")
    try src.getLines.find(_ => true)
    finally src.close()
  }

  def writeToCsv(writableDataset: DataFrame, file: String, separator: String = ",", append: Boolean = true): Unit = {
    // assure file is created on the first run
    new File(file).createNewFile()
    val target = new BufferedWriter(new FileWriter(file, append))
    try {
      writableDataset.collect.toSeq.foreach { x =>
        val row = x.mkString(separator).concat("\n")
        target.write(row)
      }
    }
    finally target.close()
  }

  /*
  Kind of dirty, but it works :D
  */
  def removeFirstLineFromFile(filename: String): Unit = {
    val source = Source.fromFile(filename)
    try {
      val lines = source
        .getLines
        .toList
        .drop(1)
      Files.write(Paths.get(filename), scala.collection.JavaConversions.asJavaIterable(lines))
    }
    finally source.close()
  }

}
