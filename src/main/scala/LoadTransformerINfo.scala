import java.util.Properties

import org.apache.spark.sql.SparkSession

object LoadTransformerINfo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("LoadTransformerINfo")
      .master("local[*]")
      .getOrCreate()
    val readDF = spark.read.csv("hdfs://master:9999/user/hadoop-twq/jl/transformer20190419.csv")
        .toDF("mp_id","bl","capacity")
        .selectExpr("cast(mp_id as Long)","bl","cast(capacity as Double)")
    readDF.printSchema()
    readDF.show()

    val mysqlUrl = "jdbc:mysql://master:3306/jltest"
    val tableName = "b_jl_transformer"
    val properties = new Properties()
    properties.put("user", "root")
    properties.put("password", "root")
    readDF.write.mode("overwrite").jdbc(mysqlUrl,tableName,properties)
  }
}
