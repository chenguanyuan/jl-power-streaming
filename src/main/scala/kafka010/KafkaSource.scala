package kafka010

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.log4j.Logger
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategy}

/**
  *  zkCli.sh -server master:2181
  *  create /real_time_session ""
  */
object KafkaSource {
  private val logger = Logger.getLogger(KafkaSource.getClass)
  def createDirectStream[K, V](ssc: StreamingContext, locationStrategy: LocationStrategy,
      kafkaParams: Map[String, Object], zkHosts: String, zkPath: String, topics: String
    ): InputDStream[ConsumerRecord[K, V]] = {

    //获取zk的offset
    val broker = kafkaParams("bootstrap.servers").toString
    val getKafkaTopicOffset = new GetKafkaTopicOffset(broker)
    val offsetsStore = new ZooKeeperOffsetsStore(zkHosts, zkPath,getKafkaTopicOffset)
    //读取zookeeper中的fromoffset数据
    val storedOffsets:Option[Map[TopicPartition,Long]] = offsetsStore.readOffsets(topics)
    val topicSet = topics.split(",").toSet
    val kafkaStream = storedOffsets match {
      case None =>
        //根据auto.offset.reset直接读取kafka中的数据
        logger.info("Create DerictStream from KafkaUtils.createDirectStream with latest offset")
        KafkaUtils.createDirectStream[K, V](ssc, locationStrategy,
          ConsumerStrategies.Subscribe[K, V](topicSet, kafkaParams))
      case Some(fromOffsets) =>
        //从zookeeper中读取offset，再读取kafka中的数据
        logger.info(s"Create DerictStream from KafkaUtils.createDirectStream with ${fromOffsets.toString()}")
        KafkaUtils.createDirectStream[K, V](ssc, locationStrategy,
          ConsumerStrategies.Subscribe[K, V](topicSet, kafkaParams, fromOffsets))
    }

    //每次读取完kafka中的数据后，把fromoffset保存到kafka中,对于空的rdd则跳过不进行保存操作
//    kafkaStream.foreachRDD(rdd =>
//      if(rdd.count() >0) offsetsStore.saveOffsets(rdd))

    kafkaStream
  }
}
