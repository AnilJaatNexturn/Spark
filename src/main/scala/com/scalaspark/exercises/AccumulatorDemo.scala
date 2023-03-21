package com.scalaspark.exercises

import org.apache.spark.sql.SparkSession

object AccumulatorDemo {
  def main(args: Array[String]): Unit = {
    val spark=SparkSession.builder()
      .appName("Accoumulation")
      .master("local[1]")
      .getOrCreate()
    val longac=spark.sparkContext.longAccumulator("SumAccountltor")
    val rdd=spark.sparkContext.parallelize(Array(1,2,3,4))
    rdd.foreach(x=>longac.add(x))
    println(longac.value)
    scala.io.StdIn.readLine()
  }

}
