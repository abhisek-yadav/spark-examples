package com.spark.test

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

object CSVReaderApp {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder().appName("CSVReaderApp")
      .master("local[*]")
      .getOrCreate()


    val schemaDef: StructType = StructType(List(
      StructField("Index", IntegerType),
      StructField("Year", IntegerType),
      StructField("Age", IntegerType),
      StructField("Name", StringType),
      StructField("Movie", StringType)
    ))

    val femaleCelebDF: DataFrame = spark.read.format("csv")
      .option("path", "./data/oscar_female_celebs.csv")
      .option("header", true)
      //      .option("inferSchema", true)
      .schema(schemaDef)
      .load

    femaleCelebDF.show(5)

    println(femaleCelebDF.schema.toString)

    spark.stop()

  }

}
