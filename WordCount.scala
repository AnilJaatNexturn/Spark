import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, explode,split}


object WordCount {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("WordCount")
      .master("local[3]") // Use all available cores
      .getOrCreate()


    val inputPath = "src/main/repo/data.txt"
    val textDF = spark.read.text(inputPath)

    val wordsDF = textDF.select(explode(split(col("value"), ",")).alias("word"))

    val wordCountsDF = wordsDF.groupBy("word").count()

    val sortedWordCountsDF = wordCountsDF.orderBy(col("count").desc)

    sortedWordCountsDF.show()

  }

}
