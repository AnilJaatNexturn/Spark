import org.apache.spark.sql.SparkSession

object BroadCasrDemo {
  def main(args: Array[String]): Unit = {
    val spark=SparkSession.builder()
      .appName("BroadCast Demo")
      .master("local[1]")
      .getOrCreate()
    val inputRdd =spark.sparkContext.parallelize(Seq(("Emp1","1000","USA","NY"),("Emp2","2000","IND","TS"),
      ("Emp3","3000","IND","TN"),("Emp4","4000","USA","TX"),("Emp5","5000","AUS","QUE")))
    val contries=Map(("USA","United State Of America"),("IND","India"),("AUS","Australia"))
    val states=Map(("NY","New Yark"),("TS","Telangana"),("TN","Tamilnadu"),("TX","Texesa"),("QUE","Queens"))
    val broadcaststates=spark.sparkContext.broadcast(states)
    val broadcastcountries=spark.sparkContext.broadcast(contries)
    val finalRDD=inputRdd.map(f=> {
      val country =f._3
      val state=f._4

      val fullstate=broadcaststates.value.get(state).get
      val fullcountry=broadcastcountries.value.get(country).get
      (f._1,f._2,fullcountry,fullstate)

    })
    println(finalRDD.collect().mkString("\n"))
    scala.io.StdIn.readLine()


  }

}
