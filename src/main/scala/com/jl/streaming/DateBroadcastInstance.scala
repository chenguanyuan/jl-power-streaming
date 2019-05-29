package com.jl.streaming

import org.apache.log4j.Logger
import org.apache.spark.broadcast.Broadcast

object DateBroadcastInstance {
  private val logger = Logger.getLogger(DateBroadcastInstance.getClass)
  /**
    * dateArrBr 记录当天所有数据的日期，日期格式为yyyyMMdd
    */
  @volatile private var dateArrBr:Broadcast[Array[String]] = _

  /**
    * 把每一批数据的日期广播，
    * 然后重新广播到executors中。
    * @param sc SparkContext
    * @param zkHosts  zookeeper的连接地址
    * @param zkPath //存放更新时间的znode
    */
  def UpdateDateArrBr(sc:org.apache.spark.SparkContext,
                      dateArr: Array[String],
                      blocking:Boolean = false):Unit={
    logger.info("dateArr broadcast is updating......")
    //如果dateArrBr不为空则先unpersist
    if (dateArrBr != null) dateArrBr.unpersist(blocking)
    //如果dateArrBr为空证明广播变量已被unpersist或刚启动程序，进行date广播
    synchronized {dateArrBr = sc.broadcast(dateArr)}
  }

  /**
    * 获取broadcast
    * @param sc
    * @return
    */
  def getInstance(sc: org.apache.spark.SparkContext,dateArr: Array[String],blocking:Boolean = false)
  : Broadcast[Array[String]] = {
    UpdateDateArrBr(sc,dateArr,blocking)
    dateArrBr
  }

}
