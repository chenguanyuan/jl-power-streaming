package com.jl.structuredstreaming

import org.apache.spark.sql.SparkSession

object JLTransformerLoadRateSparkSQL {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("JLTransformerLoadRateSparkSQL")
      .master("local[*]")
      .config("spark.sql.shuffle.partitions","40")
      .config("spark.sql.adaptive.enabled","true")
      .getOrCreate()

    val brokers = "master:9092,slave1:9092,slave2:9092"
    val topic = "test-1"

    //group.id不能设置，structured streaming-kafkaSource会自动创建唯一的group.id
    //auto.offset.reset不能设置，startingOffsets取代了这个参数
    //key.deserializer不能设置，默认key读取时被序列化成ByteArrayDeserializer二进制
    //value.deserializer不能设置，默认value读取时被序列化成ByteArrayDeserializer二进制
    //enable.auto.commit不能设置，offset由structured streaming自己管理，不会提交给kafka


    val kafkaParams = Map[String, String](
      "kafka.bootstrap.servers" -> brokers,
      "subscribe" -> topic,
      "startingOffsets" -> "latest"
    )

    //读取kafka数据
    val kafkaDF = spark.readStream.format("kafka").options(kafkaParams).load()

    spark.stop()
  }
}
