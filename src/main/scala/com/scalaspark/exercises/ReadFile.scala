package com.scalaspark.exercises

import org.apache.spark.sql.SparkSession

object ReadFile {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Read Text File")
      .master("local[1]")
      .getOrCreate()
    val rdd = spark.sparkContext.textFile("data/*")
    rdd.foreach(println)
  }
}
