import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}
import java.util.concurrent.TimeUnit

import org.apache.spark.sql.{Dataset, SparkSession}

object test {
  def main(args: Array[String]): Unit = {
    import java.text.SimpleDateFormat

    val ts = new Timestamp(1555375500000L)
    val sdf = new SimpleDateFormat("ss")
    val str = sdf.format(ts)
    println(str)
  }
}
