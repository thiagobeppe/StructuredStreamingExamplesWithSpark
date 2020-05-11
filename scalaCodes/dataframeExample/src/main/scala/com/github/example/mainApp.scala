package com.github.example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{lit, expr}

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

  println(data.select("age").show(5))

  //Criaçao de uma nova coluna
  val data_2 = data.withColumn("A_New_Column", lit(1))
  println(data_2.printSchema())
  println(data_2.select("A_New_Column").show(10))

  val test = expr("age > 40")
  //Adding a new columns with some filter
  val data_with_teste = data.select("age", "y").withColumn("teste", test)
  val new_data = data_with_teste.selectExpr("teste as MoreThan40")
  data_2.drop("A_New_Column")
  println(new_data.printSchema())
}
