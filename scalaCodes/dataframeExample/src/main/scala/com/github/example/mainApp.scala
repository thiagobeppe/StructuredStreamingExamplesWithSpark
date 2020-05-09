package com.github.example

import org.apache.spark.sql.SparkSession

import com.github.example.utils.utils.setupLogging

object mainApp extends App {
  val spark = SparkSession
    .builder
      .master("local")
    .appName("StructuredNetworkWordCount")
    .getOrCreate()

  setupLogging()

  val data = spark
                .read
                .option("header", "true")
                .option("inferSchema", "true")
                .option("delimiter", ";")
                .format("csv")
                .load("./../../data/bank-additional-full.csv")
  println(data.printSchema())

}
