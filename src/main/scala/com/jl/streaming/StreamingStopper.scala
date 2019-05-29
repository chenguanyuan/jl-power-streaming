package com.jl.streaming

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.streaming.StreamingContext

/**
  * 这是正确优雅地停止sparkstreaming程序的方法，就是把StreamingContext的stop方法中，
  * stopSparkContext和stopGracefully两个参数全部设置为true
  * 则可以优雅地停止sparkstreaming的程序。
  * 本程序的实现思想：
  * 阻塞checkIntervalMillis长时间，判断sparkstreaming程序有没有停止，如果停止了则把sparkstreaming给stop掉
  * 或者是经过checkIntervalMillis长时间后，isShutdownRequested判断hdfs指定文件是否存在，如果存在则将sparkstreaming停止
  */

trait StreamingStopper {
  // check every 10s for shutdown hdfs file
  val checkIntervalMillis = 10000
  var isStopped = false

  val shutdownFilePath = Option(System.getProperty("web.streaming.shutdown.filepath"))
    .getOrElse(sys.error("web.streaming.shutdown.filepath can not be null"))

  def stopContext(ssc: StreamingContext) = {
    while (!isStopped) {
      val isStoppedTemp = ssc.awaitTerminationOrTimeout(checkIntervalMillis)
      if (!isStoppedTemp && isShutdownRequested) {
        val stopSparkContext = true
        val stopGracefully = true
        isStopped = true
        ssc.stop(stopSparkContext, stopGracefully)
        //停止spark streaming后把停止文件给删除
        val fs = FileSystem.get(new Configuration())
        fs.delete(new Path(shutdownFilePath),true)
      }
    }
  }

  def isShutdownRequested(): Boolean = {
    val fs = FileSystem.get(new Configuration())
    fs.exists(new Path(shutdownFilePath))
  }
}
