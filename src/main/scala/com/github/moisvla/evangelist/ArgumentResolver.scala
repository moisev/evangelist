package com.github.moisvla.evangelist

import com.typesafe.scalalogging.LazyLogging

trait ArgumentResolver extends LazyLogging{

  val usage = "Usage: [input file path] [output-file path]"

  type OptionMap = Map[Symbol, Any]

  /*
  Can't use this, don't really understand why there's a conflict in BashOperator - Airflow
  */
  def nextOption(map : OptionMap, list: List[String]) : OptionMap = {
    list match {
      case Nil => map
      case "--input-file" :: value :: tail =>
        nextOption(map ++ Map('input -> value.toString), tail)
      case "--output-file" :: value :: tail =>
        nextOption(map ++ Map('output -> value.toString), tail)
      case option :: _ => throw IncompleteArgumentList(s"Unknown option $option")
    }

  }
  protected def resolveArgs(args: Array[String]): (String, String) = {
    //val options = nextOption(Map(),args.toList)
    //if (options.keySet.isEmpty) throw IncompleteArgumentList(s"Missing all arguments\n$usage")
    if (args.length != 2) throw IncompleteArgumentList(s"Missing arguments\n$usage")
    (args(0), args(1))
//    (
//      options.getOrElse('input, throw IncompleteArgumentList("Missing argument --input-file")).toString,
//      options.getOrElse('output, throw IncompleteArgumentList("Missing argument --output-file")).toString
//    )

  }

}
