package com.scalaspark.exercises
import org.apache.spark.sql.SparkSession
object CreateRdd {
  def main(args: Array[String]): Unit = {
    val spark=SparkSession.builder()
      .appName("My FirstRdd")
      .master("local[1]")
      .getOrCreate()
    val rdd=spark.sparkContext.parallelize(Seq(("anil",1),("jat",2),("asd",3)))
    rdd.foreach(println)



    //println("anil")


  }
}
