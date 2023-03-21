package com.scalaspark.exercises


import org.apache.spark.sql.SparkSession

object CsvFileDataFrame {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("DF Demo")
      .master("local[1]")
      .getOrCreate()
    val df=spark.read.option("header",true)
      .csv("data/dataframe.csv")
    df.show()
    //df.select("name","age").show()
    df.select("name","age").where("age>21").show()
  }

}
