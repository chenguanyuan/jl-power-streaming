package kafka010

import java.util.Properties

import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition

/**
  * 获取指定topic的每个partition最小的offset，用于判断从zk中读取的offset是否过期。
  */
class GetKafkaTopicOffset(broker:String) {
  private var brokers:String = broker

  private val props = new Properties()
  props.put("bootstrap.servers", brokers)
  props.put("group.id", "offsetHunter")
  props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  //@transient表示在序列化时该变量不会被序列化
  @transient
  private val consumer = new KafkaConsumer(props)

  def earliest_off(topic:String): Long = {
    import collection.JavaConverters._
    val tpMap = consumer.partitionsFor(topic).asScala.map { t =>
      val tp = new TopicPartition(t.topic, t.partition)
      consumer.assign(Seq(tp).asJava)
      consumer.seekToBeginning(Seq(tp).asJava)
      tp -> consumer.position(tp)
    }.toMap

    tpMap.map{case(_,offset)=> offset}.min
  }

  //获取指定topic的每个Partition最新的offset
  def lastest_off(topic:String): Map[TopicPartition,Long] = {
    import collection.JavaConverters._
    consumer.partitionsFor(topic).asScala.map { t =>
      val tp = new TopicPartition(t.topic, t.partition)
      consumer.assign(Seq(tp).asJava)
      consumer.seekToEnd(Seq(tp).asJava)
      tp -> consumer.position(tp)
    }.toMap
  }

  //检查zk中的offset是否与kafka指定的topic每个partition的最新offset相等
  def checkzkOffset(broker:String,tpOffset:Map[TopicPartition,Long])={
    brokers = broker
  }

  def compareOffset(broker:String,tpOffset:Map[TopicPartition,Long]):Map[String,Boolean]={
    brokers=broker
    val inputTp = tpOffset.map{case(tp,offset)=>
      (tp.topic(),offset)
    }.groupBy{case(tp,_)=>tp}.map{case(tp,offsets)=>(tp,offsets.map{case(_,v)=>v}.min)}
    inputTp.map{case(tp,zkOffset)=>
      val kafkaOffset=earliest_off(tp)
      (tp,zkOffset >= kafkaOffset)
    }
  }
}
