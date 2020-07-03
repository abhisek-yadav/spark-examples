package com.spark.test

import java.util.Properties

import org.apache.spark.{SparkConf, sql}
import org.apache.spark.sql.{DataFrame, DataFrameReader, SparkSession}

import scala.io.Source
import org.apache.spark.sql.functions.col

object SparkApp extends Serializable {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().config(getSparkConf).getOrCreate()


    val femaleCelebDF = readCSVFile(spark, "./data/oscar_female_celebs.csv")

    val femaleCelebPartitionedDF = femaleCelebDF.repartition(4);

    val celebCount = countByYear(femaleCelebPartitionedDF);

    println(celebCount.orderBy(col("count").desc).collect().mkString("->"))

    scala.io.StdIn.readLine()

    spark.stop()

  }


  def readCSVFile(spark: SparkSession, filePath: String): DataFrame = {

    spark.read
      .option("header", true)
      .option("inferSchema", true)
      .csv(filePath)

  }

  def countByYear(femaleCelebDF: DataFrame): DataFrame = {

    femaleCelebDF.where("Age < 100")
      .select("Name", "Year", "Age", "Movie")
      .groupBy("Name")
      .count()

  }

  def getSparkConf: SparkConf = {

    var props = new Properties
    props.load(Source.fromFile("spark.conf", "UTF-8").bufferedReader)

    var sparkConf = new SparkConf
    props.forEach(((k, v) => sparkConf.set(k.toString, v.toString)))

    sparkConf
  }

}
