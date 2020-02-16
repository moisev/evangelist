# ING job interview project

## Added the run_spark_bash.py DAG file in this repo
The two jobs are written in Scala (using the Spark API).
The first job will consume from an input JSON and write to an output CSV.
The second job will consume from an input CSV, compute the five most popular topics(most encountered words) and write them into another file.

# Getting Started

## 1. Build
Build the jar in the following way

    sbt assembly

## 2. Run
To run the jobs:

    spark-submit --class com.github.moisvla.evangelist.JobOne [jar_path] [input_file_path] [output_file_path]
    spark-submit --class com.github.moisvla.evangelist.JobTwo [jar_path] [input_file_path] [output_file_path]

