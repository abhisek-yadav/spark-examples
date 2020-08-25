package com.spark.test

import org.apache.spark.sql.{DataFrame, SparkSession}

object BookingApp {

  def main(args: Array[String]): Unit = {

    val sparkSession: SparkSession = SparkSession.builder().appName("BookingApp").master("local[*]").getOrCreate()

    val hotelsDF: DataFrame = sparkSession.read
      .option("header", true)
      .option("inferSchema", true).
      csv("./data/hotels.csv")

    println(hotelsDF.show())

    val count = hotelsDF.select("City", "Name").groupBy("City", "Name").count()

    println(count.show())

    val city = count.select("City").where("Count >=3")

    println(city.show())

  }
}
