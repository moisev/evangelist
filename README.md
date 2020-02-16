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
# 2.1. Run the jobs locally:

    spark-submit --class com.github.moisvla.evangelist.JobOne [jar_path] [input_file_path] [output_file_path]
    spark-submit --class com.github.moisvla.evangelist.JobTwo [jar_path] [input_file_path] [output_file_path]

# 2.2. Run the apps by using a Docker Container (Spark needs to be installed)
    Copy the Evangelist-assembly-0.1.jar in the *files* mounted folder
    Copy the run_spark_bash.py DAG file in the *dag* mounted folder
    Turn on the run_spark_with_bash DAG in the Airflow UI