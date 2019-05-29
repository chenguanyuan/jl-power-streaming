package utils

import java.sql.Timestamp

import com.jl.streaming.transformerCurrentStatus
import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import org.apache.spark.rdd.RDD
import redis.clients.jedis.{JedisPool, Pipeline}

//test
object RedisStatusManager {

  def saveRDDStatus(rdd:RDD[(Long,transformerCurrentStatus)]):Unit={
    rdd.foreachPartition { iter =>
      val jedis = InternalRedisClient.getPool.getResource
      val pipeline = jedis.pipelined()
      iter.foreach { case (key, status) =>
        pipeline.set(key.toString, status2String(status))
      }
      pipeline.sync()
      InternalRedisClient.getPool.returnResource(jedis)
    }
  }

  def initializeRDD(rdd:RDD[(Long,List[(Timestamp,Array[Double])])]):Option[RDD[(Long,transformerCurrentStatus)]]={
    println("initialize。。。。。。")
    val initRDD = rdd.mapPartitions{ iter =>
      println("redis初始化RDD。。。。。。")
      val jedis = InternalRedisClient.getPool.getResource
      val pipeline = jedis.pipelined()

      val pipelineGet = iter.toMap.keySet.map{mpid=>(mpid,pipeline.get(mpid.toString))}
      pipeline.sync()
      val dataSet = pipelineGet.map{case(mpid,pGet)=>
        val value = pGet.get()
        if(Option(value).isEmpty){
          (mpid,new transformerCurrentStatus(mp_id = mpid))
        }else{(mpid,string2Status(mpid,value))}
      }
      InternalRedisClient.getPool.returnResource(jedis)
      dataSet.iterator
    }
    Some(initRDD)
  }

  def readStatus(keySet:Array[Long],pipeline: Pipeline):Map[Long,transformerCurrentStatus]={
    val pipelineGet = keySet.map{key=>(key,pipeline.get(key.toString))}
    pipeline.sync()
    val statusSet = pipelineGet.map{case(key,get)=>
      val value = get.get()
      if(Option(value).isEmpty){
        (key,new transformerCurrentStatus(mp_id = key))
      }else{(key,string2Status(key,value))}
    }
    statusSet.toMap
  }

  def status2String(status:transformerCurrentStatus):String={
    //val key:String = status.mp_id.toString
    val eventTime = status.time.getTime.toString
    val startArray = status.startArray.map{time=>time.getTime.toString}.mkString(",")
    val durationArray = status.durationArray.map{time=>time.toString}.mkString(",")
    val status1 = status.status.toString
    val overLoadstatus = status.overLoadstatus.toString
    val loadRate = status.loadRate.toString
    val value = eventTime+ "\\|\\|" + startArray + "\\|\\|" + durationArray + "\\|\\|" + status1 + "\\|\\|" + overLoadstatus + "\\|\\|" + loadRate
    value
  }

  def string2Status(key:Long,value:String): transformerCurrentStatus={
    //val longKey = key.toLong
    val valueArr = value.split("\\|\\|")
    val eventTime = new Timestamp(valueArr(0).toLong)
    val startArray = valueArr(1).split(",").map{millsTime=> new Timestamp(millsTime.toLong)}
    val durationArray = valueArr(2).split(",").map{millsTime=>millsTime.toLong}
    val status = valueArr(3).toByte
    val overLoadstatus = valueArr(4).toByte
    val loadRate = valueArr(5).toDouble
    new transformerCurrentStatus(key,eventTime,startArray,durationArray,status,overLoadstatus,loadRate)
  }
}



/**
  * 需要的依赖包
  * <dependency>
  * <groupId>redis.clients</groupId>
  * <artifactId>jedis</artifactId>
  * <version>2.5.2</version>
  * </dependency>
  * <!--jedis的连接池就是基于Apache commons-pool2实现-->
  * <dependency>
  * <groupId>org.apache.commons</groupId>
  * <artifactId>commons-pool2</artifactId>
  * <version>2.2</version>
  * </dependency>
  *
  * Internal Redis client for managing Redis connection {@link Jedis} based on {@link RedisPool}
  */
object InternalRedisClient extends Serializable {

  //@transient被该注解注释的变量不会被序列化
  @transient private lazy val pool: JedisPool = {
    val maxTotal = 20
    val maxIdle = 10
    val minIdle = 1
    val redisHost = "slave1"
    val redisPort = 6379
    val redisTimeout = 30000
    val poolConfig = new GenericObjectPoolConfig()
    //设置最大连接数
    poolConfig.setMaxTotal(maxTotal)
    //最大空闲连接数，一般等于maxTotal
    poolConfig.setMaxIdle(maxIdle)
    //最小空闲连接数
    poolConfig.setMinIdle(minIdle)
    //在获取连接的时候检查有效性
    poolConfig.setTestOnBorrow(true)
    //在归还连接的时候检查有效性
    poolConfig.setTestOnReturn(false)
    //获取连接时的最大等待毫秒数(如果设置为阻塞时BlockWhenExhausted),如果超时就抛异常, 小于零:阻塞不确定的时间,  默认-1
    poolConfig.setMaxWaitMillis(100000)


    val hook = new Thread {
      override def run = pool.destroy()
    }
    //addShutDownHook 的作用就是：在你的程序结束前，执行一些清理工作，尤其是没有UI的程序。
    sys.addShutdownHook(hook.run)

    new JedisPool(poolConfig, redisHost, redisPort, redisTimeout)
  }

  def getPool: JedisPool = {
    assert(pool != null)
    pool
  }
}
