package com.github.example

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions.{col, expr, lit}
import com.github.example.utils.utils.setupLogging
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

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

  println(data.select("age").show())

  // ### Aula 02 ###
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


  // ## Aula 3 - Filtragem ##
  println(data.select("age", "job").filter(col("age")>55).orderBy(col("age").desc).show(2))
  println(data.select("age", "marital").filter(col("marital").equalTo("married")).show(5))

  // Três formas de fazer a mesma coisa
  println(data.select("age", "marital").where(col("marital")=!="married").show(5))
  println(data.select("age", "marital").filter(col("marital")=!="married").show(5))
  println(data.select("age", "marital").where("marital <> 'married'").show(5))

  //Distinct
  println(data.select("marital").distinct().show())

  //Multiplos Filtros
  val filtro_idade = col("age") > 40
  val filtro_civil = col("marital").contains("married")
  println(data.select("age", "job", "marital").where(col("job").isin("unemployed", "retired")).where(filtro_civil.and(filtro_idade)).show(5))


  // Aula 04

  val simpleQuery = data.selectExpr("age + 10", "age").orderBy("age").show(10)

  val seed = 2019
  val withReplacement = false
  val fraction = 0.1
  data.sample(withReplacement,fraction,seed).show()


  //aula 05 missing data & creating a DataFrame

  val schema = new StructType(Array(
    StructField("age", IntegerType, true),
    StructField("job", StringType , true)
  ))

  val newRoews = Seq(Row(30, "DS"), Row(20, "Dev Java" ), Row(10, null
  ))

  val parallelizeRows = spark.sparkContext.parallelize(newRoews)
  val dados_manual = spark.createDataFrame(parallelizeRows, schema)

  dados_manual.show()

  //Missing
  dados_manual.select("job","age").na.drop("all").show() // Somente se TODAS estiverem null
  dados_manual.select("job","age").na.drop("any").show() // Se uma das duas
  dados_manual.na.fill("Desconhecido").show()


}
