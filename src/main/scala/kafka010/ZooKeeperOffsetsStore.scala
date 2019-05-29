package kafka010

import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.kafka.common.TopicPartition
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.slf4j.LoggerFactory
import utils.Stopwatch

import scala.collection.mutable

/**
  *  将Spark Streaming消费的kafka的offset信息保存到zookeeper中
  * @param zkHosts zookeeper的主机信息
  * @param zkPath offsets存储在zookeeper的主路径
  * @param getKafkaTopicOffset 用于获取kafka指定topic的offset，默认值为Null
  */
class ZooKeeperOffsetsStore(zkHosts: String, zkPath: String,getKafkaTopicOffset:GetKafkaTopicOffset = null) extends OffsetsStore with Serializable {
  private val logger = LoggerFactory.getLogger("ZooKeeperOffsetsStore")

  //@transient表示在序列化时该变量不会被序列化
  @transient
  private val client = CuratorFrameworkFactory.builder()
    .connectString(zkHosts)
    .connectionTimeoutMs(10000)
    .sessionTimeoutMs(10000)
    .retryPolicy(new ExponentialBackoffRetry(1000, 3))
    .build()
  client.start()

  //若zkPath不存在，则创建zkPath目录，且内容为空
  if (client.checkExists.forPath(zkPath) == null){
    client.create.creatingParentsIfNeeded.forPath(zkPath,"".getBytes())
  }

  override def readOffsets(): Option[Map[TopicPartition, Long]] =None

  /**
    *  从zookeeper上读取Spark Streaming消费的指定topic的所有的partition的offset信息
    *  zkPath-|
    *         |topic1    partition1:fromoffset1,partition2:fromoffset2
    *         |topic2    partition1:fromoffset1,partition2:fromoffset2
    *         |topic3    partition1:fromoffset1,partition2:fromoffset2
    *
    * @param topic "topic1,topic2,topic3"
    * @return
    */
  override def readOffsets(topic:String): Option[Map[TopicPartition, Long]] = {
    logger.info("Reading offsets from ZooKeeper")
    val stopwatch = new Stopwatch()
    val topicSet = topic.split(",").toSet
    val topicPartitionsSet:Set[Option[Map[TopicPartition,Long]]] = topicSet.map{aloneTopic=>
      //若zkPath\topic不存在，则创建zkPath目录，且内容为空
      if (client.checkExists.forPath(zkPath+"/"+aloneTopic) == null){
        client.create.creatingParentsIfNeeded.forPath(zkPath+"/"+aloneTopic,"".getBytes())
      }
      //(partition1:220,partition2:320,partition3:10000)
      val offsetsRangesStrOpt:Option[String] = Some(new String(client.getData.forPath(zkPath+"/"+aloneTopic)))

      offsetsRangesStrOpt match {
        case Some(offsetsRangesStr) =>
          logger.info(s"Read offset ranges: ${offsetsRangesStr}")
          if (offsetsRangesStr.isEmpty) {
            None
          } else {
            val offsets:Map[TopicPartition,Long] = offsetsRangesStr.split(",")
              .map{s => val arr = s.split(":")
                (arr(0),arr(1))
              }
              .map { case (partitionStr, offsetStr) =>
                //从kafka指定topic获取每个partition对应的最新offset，如果最新offset大于zk保存的offset
                //则从读取kafka中offset，如果等于则设置为-1并过滤掉
                val tp = new TopicPartition(aloneTopic, partitionStr.toInt)
                if(getKafkaTopicOffset == null){
                  throw new Exception("getKafkaTopicOffset is null")
                }
                val lastet_tp:Map[TopicPartition,Long] = getKafkaTopicOffset.lastest_off(aloneTopic)
                val lastestOffset:Long = lastet_tp(tp)
                if(lastestOffset > offsetStr.toLong) tp -> offsetStr.toLong
                else tp -> -1L
                }.filter(p=>p._2 != -1)
                //new TopicPartition(aloneTopic, partitionStr.toInt) -> offsetStr.toLong}
              .toMap
             if(offsets.isEmpty) None else Some(offsets)
          }
        case _ => None
      }
    }.filter(_.nonEmpty)

    if (topicPartitionsSet.nonEmpty){
      var offsetMap = mutable.Map[TopicPartition,Long]()
      topicPartitionsSet.foreach {opt=>offsetMap ++= opt.get}

      logger.info("Done reading offsets from ZooKeeper. Took " + stopwatch)
      Some(offsetMap.toMap)
    }else {
        logger.info(s"No offsets found in ZooKeeper:$zkPath. Took " + stopwatch)
      None
    }
  }

  /**
    *  将指定的topic的所有的partition的offset信息保存到zookeeper中
    * @param topic
    * @param rdd
    */
  override def saveOffsets(rdd: RDD[_]): Unit = {
    logger.info("Saving offsets to ZooKeeper")
    val stopwatch = new Stopwatch()

    val offsetsRanges:Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

    offsetsRanges.foreach{offsetRange => logger.info(s"Using ${offsetRange}")
    }

    //(topic,partition1:220,partition2:320,partition3:10000)
    val offsetsRangesStr = offsetsRanges.map{offsetRange =>
      (offsetRange.topic,s"${offsetRange.partition}:${offsetRange.untilOffset}")}
      .groupBy{case(key,_)=>key}.map{case(topic,value)=>(topic,value.map(ele=>ele._2).mkString(","))}

    logger.info(s"Writing offsets to ZooKeeper: ${offsetsRangesStr}")
    offsetsRangesStr.foreach{case(topic,partitionFromoffset)=>
      client.setData().forPath(zkPath+"/"+topic, partitionFromoffset.getBytes())
    }
    logger.info("Done updating offsets in ZooKeeper. Took " + stopwatch)
  }
}
