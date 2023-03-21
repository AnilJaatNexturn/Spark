package com.scalaspark.exercises

//import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages.SparkAppConfig
import org.apache.spark.sql.SparkSession

object MapDemo {
  def main(args: Array[String]): Unit = {
    val spark=SparkSession.builder()
      .appName("Demo")
      .master("Local[1]")
      .getOrCreate()
    val data=Seq("project Gutenger",
      "Alice is advantage in WonderLang",
    "project Gutenbedfr",
    "Advantage in WonderLang",
    "project gutengers")
    val rddmap = data.map()


  }

}
