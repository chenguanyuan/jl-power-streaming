package com.jl.streaming

import java.util.regex.Pattern
import java.util.{Calendar, Date}

import com.DBLinkPool.ConnectionPool
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.log4j.Logger
import org.apache.spark.broadcast.Broadcast

/**
  * 用于获取数据库中配变的倍率、容量等基本信息，然后通过broadcast广播到每个executor中
  */
object TranformerBroadcastInstance extends  Serializable {


  /**
    * transBr 配变transformer参数信息广播变量，每个元素为(key,Array(bl,capacity))
    * 用volatile修饰的变量，线程在每次使用变量的时候，都会读取变量修改后的值
    * map 从数据库中读取tranformer表的数据信息，包括mpid、bl(综合倍率)、capacity(配变容量)
    */
  @volatile private var transBr:Broadcast[Map[Long,Array[Double]]] = _
  private val map = scala.collection.mutable.Map[Long,Array[Double]]()
  private val logger = Logger.getLogger(TranformerBroadcastInstance.getClass)

  private def getDBData():Map[Long,Array[Double]]={
    val conn = ConnectionPool.getConnection
    val statement = conn.prepareStatement(s"select mp_id, bl, capacity from b_jl_transformer")
    statement.setFetchSize(700000)
    val rs = statement.executeQuery()
    while (rs.next()) {
      val blRead = if(rs.getString(2) != null) rs.getString(2) else "null"
      val isNumber = Pattern.matches("^[0-9]\\d*$",blRead.trim)
      val bl:Double =if(isNumber)rs.getString(2).toDouble else 0D
      map += (rs.getLong(1) -> Array(bl, rs.getDouble(3)))
    }
    rs.close()
    statement.close()
    ConnectionPool.returnConnection(conn)
    map.toMap
  }

  /**
    * 检查配变信息表是否被广播。程序刚启动或到了zkPath设定的时间，把配变信息表更新，
    * 然后重新广播到executors中。
    * @param sc SparkContext
    * @param zkHosts  zookeeper的连接地址
    * @param zkPath //存放更新时间的znode
    */
  def checkAndUpdateTransformer(sc:org.apache.spark.SparkContext,
                     zkHosts:String,
                     zkPath:String,
                     blocking:Boolean = false):Unit={
    val cal = Calendar.getInstance
    cal.setTime(new Date)
    var dateTime:Int = 2
    //从zk读取transformer表更新的时间
    @transient
    val client = CuratorFrameworkFactory.builder()
      .connectString(zkHosts)
      .connectionTimeoutMs(10000)
      .sessionTimeoutMs(10000)
      .retryPolicy(new ExponentialBackoffRetry(1000, 3))
      .build()
    client.start()

    if (client.checkExists.forPath(zkPath) != null){
      dateTime = (new String(client.getData.forPath(zkPath))).toInt
    }
    //周dateTime+1更新配变信息
    if(transBr == null ||(cal.get(Calendar.DAY_OF_WEEK) - 1)== dateTime) {
      if (transBr != null) {
        transBr.unpersist(blocking)
      }
      logger.info("Transfomer broadcast is updating......")
      transBr=sc.broadcast(getDBData())
    }
    client.close()
  }

  /**
    * 获取broadcast
    * @param sc
    * @return
    */
  def getInstance(sc: org.apache.spark.SparkContext): Broadcast[Map[Long,Array[Double]]] = {
    if (transBr == null) {
      synchronized {
        if (transBr == null) {
          transBr = sc.broadcast(getDBData())
        }
      }
    }
    transBr
  }

}
