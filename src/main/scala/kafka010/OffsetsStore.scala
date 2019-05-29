package kafka010

import org.apache.kafka.common.TopicPartition
import org.apache.spark.rdd.RDD

trait OffsetsStore {

  //从某个数据库读取offset信息
  def readOffsets(): Option[Map[TopicPartition, Long]]
  def readOffsets(topic: String): Option[Map[TopicPartition, Long]]

  //把offset信息保存到某个数据库中
  def saveOffsets(rdd: RDD[_]): Unit

}
