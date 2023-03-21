package com.scalaspark.exercises

import org.apache.spark.sql.SparkSession

object CsvToJasonDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Csv To Demo Demo")
      .master("local[1]")
      .getOrCreate()
    val df=spark.read.option("header",true)
      .csv("data/dataframe.csv")
    df.printSchema()
    df.write.json("data/samplejason")
  }
}
