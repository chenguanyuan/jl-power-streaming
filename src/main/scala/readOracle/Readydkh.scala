package readOracle

import java.util.Properties

import org.apache.spark.sql.SparkSession

object Readydkh {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Readydkh").getOrCreate()


    val oracleUrl = "jdbc:oracle:thin:@//10.150.20.50:1522/newgddw"

    val tablename = "(select YHBH,KHBH,YHMC,YDDZ from dw_csg.npmis_KH_YDKH) ydkh"

    val properties = new Properties()
    properties.put("driver","oracle.jdbc.driver.OracleDriver")
    properties.put("user","DYJCXT")
    properties.put("password","dyjcxt$123$")


    val readOpts = Map[String, String]( "fetchsize" -> "10000")

    val predicates = Array(
      "yhbh >= '030000' and yhbh < '030200'",
      "yhbh >= '030200' and yhbh < '030400'",
      "yhbh >= '030400' and yhbh < '030500'",
      "yhbh >= '030500' and yhbh < '030600'",
      "yhbh >= '030600' and yhbh < '030700'",
      "yhbh >= '030700' and yhbh < '030800'",
      "yhbh >= '030800' and yhbh < '030900'",
      "yhbh >= '030900' and yhbh < '031200'",
      "yhbh >= '030900' and yhbh < '0309005'",
      "yhbh >= '0309005' and yhbh < '0309008'",
      "yhbh >= '0309008' and yhbh < '031200'",
      "yhbh >= '031200' and yhbh < '031300'",
      "yhbh >= '031300' and yhbh < '031400'",
      "yhbh >= '031400' and yhbh < '031500'",
      "yhbh >= '031500' and yhbh < '031600'",
      "yhbh >= '031600' and yhbh < '031700'",
      "yhbh >= '031700' and yhbh < '031800'",
      "yhbh >= '031800' and yhbh < '031900'",
      "yhbh >= '031900' and yhbh < '032000'",
      "yhbh >= '032000' and yhbh < '035100'",
      "yhbh >= '035100' and yhbh < '035200'",
      "yhbh >= '035200' and yhbh < '035300'",
      "yhbh >= '035300' and yhbh < '035400'",
      "yhbh >= '035400' and yhbh < '062000'",
      "yhbh >= '062000' and yhbh < '063000'",
      "yhbh >= '063000' and yhbh < '064000'",
      "yhbh >= '064000' and yhbh < '065000'",
      "yhbh >= '065000'"
    )

    val oracleDF = spark.read.options(readOpts).jdbc(oracleUrl,tablename,predicates,properties)

    oracleDF.write.mode("overwrite").csv("hdfs://vms-ha/jl/ydkh")

    spark.stop()
  }
}
