package readOracle

import java.util.Properties

import org.apache.spark.sql.SparkSession

object ReadTransformerTable {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Readydkh").getOrCreate()

    val oracleUrl = "jdbc:oracle:thin:@//192.168.2.130:1521/vms"

    val tablename = "(select mp_id, bl, capacity from b_jl_transformer) transformer"
    val transformerpower = args(0)
    val outputTableName = args(1)

    val properties = new Properties()
    properties.put("driver","oracle.jdbc.driver.OracleDriver")
    properties.put("user","vmsadmin")
    properties.put("password","VmsOra#15")

    val readOpts = Map[String, String]( "numPartitions" -> "5", "partitionColumn" -> "mp_id",
      "lowerBound" -> "1000", "upperBound" -> "10000", "fetchsize" -> "5000")

    val oracleDF = spark.read.options(readOpts).jdbc(oracleUrl,transformerpower,properties)

    oracleDF.write.mode("overwrite").csv(s"hdfs://vms-ha/user/cgy/${outputTableName}")
    spark.stop()
  }
}
