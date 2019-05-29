import org.apache.spark.sql.SparkSession

object combineSmallFiles {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("combineSmallFiles").master("local[*]").getOrCreate()

    val filesDF = spark.read.textFile("E:\\bigdataCode\\jl-power-streaming\\data\\dongguan.txt")
    import spark.implicits._
    filesDF.filter{line=>
      line.replaceAll("^[^0-9]*", "").contains("586968118")
    }.coalesce(1).map{line=>
      val dateTime = line.split("\\|\\|")(1)
      (dateTime,line)
    }.toDF("dateTime","line").sort($"dateTime".asc).select("line")
    .write.mode("overwrite").text("E:\\bigdataCode\\jl-power-streaming\\data\\dg.txt")
    spark.stop()
  }
}
