package com.jl.streaming

import java.io.IOException
import java.math.RoundingMode
import java.sql.{Connection, Timestamp}
import java.text.{DecimalFormat, SimpleDateFormat}
import java.util.Properties
import java.util.regex.Pattern

import com.DBLinkPool.ConnectionPool
import databasecreatesql.{CreateMysqlTableSql, CreateOracleTableSql}
import kafka010.{KafkaSource, ZooKeeperOffsetsStore}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.log4j.Logger
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.LocationStrategies
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext, Time}
import org.apache.spark.{RangePartitioner, SparkConf, SparkContext}
import utils.{InternalRedisClient, RedisStatusManager}

import scala.collection.mutable.{HashMap, ListBuffer}

/**

创建kafka主题jlpowerTopic
  cd /usr/hdp/2.6.0.3-8/kafka
  bin/kafka-topics.sh --create --zookeeper 192.168.2.131:2181 --replication-factor 1 --partitions 1 --topic jlpowerTopic

   在master上运行计量新增数据的java程序，写入kafka的topic，jlpowerTopic：
  nohup java -cp jl-power-streaming-1.0-SNAPSHOT-jar-with-dependencies.jar com.kafka.kafkaProducer > kafkaproducer.log  2>&1 &

  执行本程序：
  通过脚本jlpowerstreaming.sh执行

  正确停止本spark streaming程序：
  hadoop fs -touchz /user/hadoop-twq/spark-course/sparkstreaming/stop
  **/

//ADASDASDASDASDASDASD
object JLTransformerLoadRate extends StreamingStopper {

  private val logger = Logger.getLogger(JLTransformerLoadRate.getClass)

  def main(args: Array[String]): Unit = {
    if (args.length < 4) {
      System.err.println(s"""
                            |Usage: JLTransformerLoadRate <brokers> <topics>
                            |  <brokers> is a list of one or more Kafka brokers,like:master:9092,slave1:9092...
                            |  <topics> is a list of one or more kafka topics to consume from
                            |  <batchIntervalStr> is the batchinterval of spark streaming
                            |  <zkHosts> is a list of one or more zookeeper hosts
        """.stripMargin)
      System.exit(1)
    }

    val Array(brokers,topics,batchIntervalStr,zkHosts) = args

    val batchArr = batchIntervalStr.split("\\s+")
    if(batchArr.length != 2){
      System.err.println(s"batchInterval ${batchIntervalStr} is wrong set.")
      System.exit(1)
    }
    val (batchTime,timeUnit) = (batchArr(0),batchArr(1))

    val batchInterval = timeUnit.toUpperCase match {
      case "SECOND" => Seconds(batchTime.toLong)
      case "MINUTE" => Minutes(batchTime.toLong)
      case _ => Minutes(3)
    }

    val sparkConf = new SparkConf()
      .setAppName("JLTransformerLoadRate")
      .set("spark.streaming.backpressure.enabled","true")

    if(!"yarn".equals(sparkConf.getOption("spark.master")
      .getOrElse("none"))) sparkConf.setMaster("local[*]")

    val sc=new SparkContext(sparkConf)
    val ssc=new StreamingContext(sc, batchInterval)


    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> brokers,
      "group.id" -> "JLTransformerLoadRateGroup",
      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean))

    //存放offset的地址
    val zkPath = "/JLTransformerLoadRate/kafka/sparkstreamingoffset"
    val offsetsStore = new ZooKeeperOffsetsStore(zkHosts, zkPath)
    //存放transformer表更新的zk地址
    val transzkPath = "/JLTransformerLoadRate/transformerupdateTime"

    //首先从zk中读取offset，根据offset再从kafka中读取数据
    logger.info("Reading kafka-offset with zookeeper......")
    val kafkaStream:DStream[ConsumerRecord[String,String]] = KafkaSource.createDirectStream[String, String](ssc: StreamingContext,
      LocationStrategies.PreferConsistent,
      kafkaParams, zkHosts, zkPath, topics)


    println("启动完毕，准备计算......")

    kafkaStream.foreachRDD{ kafkaRDD =>
      if(kafkaRDD.count() > 0) {
        val completeTaksNumber = kafkaRDD.sparkContext.longAccumulator("JLTransformerLoadRate")

        //每一行数据进行切分并去除mp_id，时间戳为空的数据，然后按照mp_id分组后按时间戳升序排序
        val messagesRDD:RDD[(Long,List[(Timestamp,Array[Double])])] = dataCleaning(kafkaRDD).cache()

        //获取所有数据的年月日并去重，用于按天建表
        val dateArr: Array[String] = messagesRDD.flatMap { case (_, timeDataIter) =>
          timeDataIter.map { case (timestamp, _) =>
            val format = new SimpleDateFormat("yyyyMMdd")
            format.format(timestamp)
          }
        }.distinct().collect
        println(dateArr.mkString(","))
        //根据日期，创建所有需要写入数据的表，创建表语句里面已有判断表是否存在
        logger.info("checking database whether to create tables.")
        checkAndCreateTable(dateArr)
        //将本批数据的年月日广播到executor
        val dateArrBr = DateBroadcastInstance.getInstance(messagesRDD.sparkContext,dateArr,true)
        //获取配变信息表的广播变量
        val tranformerBroadcast = TranformerBroadcastInstance.getInstance(messagesRDD.sparkContext)
        //根据日期，把二次值写入对应的二次值记录日表transformer_sourcedata_day_$date
        logger.info("Save secondary data to table transformer_sourcedata_day.")
        writeFirstOrSecondData(messagesRDD, "transformer_sourcedata_day",
          dateArrBr,18)

        //根据节点时间，每周二更新transformer信息数据的广播数据
        logger.info("checking date whether to update transfomer broadcast.")
        TranformerBroadcastInstance.checkAndUpdateTransformer(messagesRDD.sparkContext, zkHosts,
          transzkPath,true)

        //过滤在transBr没有匹配上的mpid记录及mpid对应的综合倍率为0的记录
        val dataFilter = messagesRDD.filter { case (mpid, _) =>
          val transInfo = tranformerBroadcast.value
          val bl:Double = if (transInfo.get(mpid).nonEmpty) transInfo(mpid)(0) else 0D
          bl != 0D
        }

        //计算一次值，并求最大功率，及负载率
        logger.info("calculating high side value and load rate.")
        val dataParser1:RDD[(Long,List[(Timestamp,Array[Double])])] = dataFilter.map { case (mpID, dataIter) =>
          val transInfo = tranformerBroadcast.value
          //bl为综合倍率，capacity为配变容量
          val (bl, capacity) = (transInfo(mpID)(0), transInfo(mpID)(1))
          val dataParse = dataIter.map { case (time, powerData) =>
            (time, calculateMaxpowerAndRate(powerData, bl, capacity)) }
          (mpID, dataParse)
        }
        dataParser1.cache()

        //把功率、功率因数、负载率写入到变压器一次值时刻功率日表transformer_primarysidedata_day
        logger.info("Save first data to table transformer_primarysidedata_day.")
        writeFirstOrSecondData(dataParser1, "transformer_primarysidedata_day",
          dateArrBr,20)

        //计算状态的时间参数，对于迟到的数据先记录功率数据，这里不对其重过载情况进行重新计算
        val dataParser2 = dataParser1.mapPartitions { iter =>
          val list = iter.toList
          //读取当前配变状态
          //val mpidSet:List[Long] = list.toMap.keySet.toList.sorted
          //val minMpid:Long = if(mpidSet.nonEmpty) mpidSet.head else 0L
          //val maxMpid:Long = if(mpidSet.nonEmpty) mpidSet.last else 0L
          val mpidSet:Array[Long] = list.toMap.keySet.toArray

          val conn = ConnectionPool.getConnection
          conn.setAutoCommit(false)


          //读取配变最新时刻状态
          val jedis = InternalRedisClient.getPool.getResource
          val pipeline = jedis.pipelined()
          val transformerStatusMap = RedisStatusManager.readStatus(mpidSet,pipeline)
          InternalRedisClient.getPool.returnResource(jedis)
          //val transformerStatusMap = readTransformerCurrentStatus(conn, minMpid,maxMpid)

          //负载率判断值数组
          val loadRateArray = Array(0.8, 1.0, 1.2, 1.3, 1.5, 1.6, 1.8)
          //该List记录需要写入重过载表中的数据
          val transExcStaList = new ListBuffer[transformerExceedStatus]()
          //该List记录需要更新transformer_ins_status_day表的记录
          val transNeedUpdateList = new ListBuffer[transformerCurrentStatus]()
          //与当前状态对比，计算时间参数
          //对每个Mpid的功率参数换算成时间参数transformerCurrentStatus
          //每个ListBuffer[transformerCurrentStatus]里面的mpid都是相同的
          val CurrentStatusList: List[(Boolean, ListBuffer[transformerCurrentStatus])] =
          calculateTransformerStatus(list, loadRateArray, transformerStatusMap, transExcStaList,transNeedUpdateList)

          //把确认重过载数据写入重过载情况写入到变压器重过载统计表transformer_exceed_status
          writeTransformerExceedStatus(conn, transExcStaList,dateArrBr)
          //对transformer_ins_status_day进行状态更新
          updatestartStatus(conn, transNeedUpdateList,dateArrBr)

          conn.commit()
          conn.setAutoCommit(true)
          ConnectionPool.returnConnection(conn)
          CurrentStatusList.iterator
        }
        dataParser2.cache()

        //把配变最新的配变状态信息写入transformer_current_status表
//        dataParser2.filter{case(_,eleList) =>eleList.last.status != 4}
//          .map {case(newMpid,eleList) => (newMpid,eleList.last)}
//          .foreachPartition { currentStatusListIter =>
//            val conn = ConnectionPool.getConnection
//            conn.setAutoCommit(false)
//            writeTransformerCurrentStatus(conn, currentStatusListIter)
//            conn.setAutoCommit(true)
//            ConnectionPool.returnConnection(conn)
//            completeTaksNumber.add(1)
//        }

        //把本次配变状态写入对应的日表transformer_ins_status_day
        dataParser2.foreachPartition { currentStatusListIter =>
          val conn = ConnectionPool.getConnection
          conn.setAutoCommit(false)
          writeTransformerInsStatusDay(conn, currentStatusListIter,dateArrBr)
          conn.setAutoCommit(true)
          ConnectionPool.returnConnection(conn)
          completeTaksNumber.add(1)
        }

        //如果每个任务全部完成，则把offset保存到zk中
        import scala.util.control.Breaks._
        breakable{
          while(!isShutdownRequested()){
            if(completeTaksNumber.value == 2 * dataParser2.partitions.size){
              offsetsStore.saveOffsets(kafkaRDD)
              break
            }
          }
        }
        completeTaksNumber.reset()
      }
    }
    //开始spark streaming
    ssc.start()
    //优雅停止spark streaming
    stopContext(ssc)
  }

  /**
    * 检查数据库中是否存在对应日期的
    * 二次值表transformer_sourcedata_day、
    * 一次值表transformer_primarysidedata_day、
    * 配变状态信息表transformer_ins_status_day、
    * 配变确认重过载表transformer_exceed_status；
    * 如果没有，则创建上述四张表。
    * @param dateArr 每一批数据的日期
    */
  def checkAndCreateTable(dateArr:Array[String]):Unit={

    val properties = new Properties
    val in = JLTransformerLoadRate.getClass.getClassLoader.getResourceAsStream("DB.properties")
    try
      properties.load(in)
    catch {
      case e: IOException =>
        e.printStackTrace()
    }
    val dbName = properties.getProperty("dbname")

    val conn = ConnectionPool.getConnection
    val statement = conn.createStatement
    dateArr.foreach {date=>
      //创建原始数据日表transformer_sourcedata_day,用于记录从计量读取的二次值
      val createSourcedataSql = if(dbName == "oracle") CreateOracleTableSql.createTransformerSourcedataDaySql(date)
      else CreateMysqlTableSql.createTransformerSourcedataDaySql(date)

      //创建一次值记录日表transformer_primarysidedata_day
      val createPrimarydataSql = if(dbName == "oracle") CreateOracleTableSql.createTransformerPrimarysidedataDaySql(date)
      else CreateMysqlTableSql.createTransformerPrimarysidedataDaySql(date)

      //创建变压器时刻状态日表transformer_ins_status_day，记录所有数据对应的时刻状态
      val createStatusSql = if(dbName == "oracle") CreateOracleTableSql.createTransformerInsStatusSql(date)
      else CreateMysqlTableSql.createTransformerInsStatusSql(date)

      //创建变压器重过载日表transformer_exceed_status，已经确认为重过载的记录
      val createExceedSql = if(dbName == "oracle") CreateOracleTableSql.createTransformerExceedStatus(date)
      else CreateMysqlTableSql.createTransformerExceedStatus(date)

      statement.addBatch(createSourcedataSql)
      statement.addBatch(createPrimarydataSql)
      statement.addBatch(createStatusSql)
      statement.addBatch(createExceedSql)
    }
    statement.executeBatch()
    statement.clearBatch()
    statement.close()
    ConnectionPool.returnConnection(conn)
  }

  /**
    * 把解析的一二次值写入对应的库表中
    * @param rdd 需要写入数据库的RDD
    * @param tablePrefixname  库表不包含时间的表名前缀
    * @param paramNum 写入库表的参数个数
    */
  def writeFirstOrSecondData(rdd:RDD[(Long,List[(Timestamp,Array[Double])])],
                             tablePrefixname:String,
                             dateArrBr: Broadcast[Array[String]],
                             paramNum:Int): Unit ={
      rdd.foreachPartition { partitionRecords =>
        val conn = ConnectionPool.getConnection
        conn.setAutoCommit(false)

          //根据日期创建不同的写入表语句
          val statementMap =dateArrBr.value.map(date=>{
            val paramMark:String = Range(0,paramNum).map(_=>"?").mkString(",")
            val statement = conn.prepareStatement(s"insert into ${tablePrefixname}_${date} " +
              s"values (${paramMark})")
            (date,statement)
          }).toMap

          partitionRecords.zipWithIndex.foreach {case ((mpid,value),index) =>
            value.foreach{case (time,powerdata)=>
              val format =new SimpleDateFormat("yyyyMMdd")
              val date = format.format(time)
              statementMap(date).setLong(1,mpid)
              statementMap(date).setTimestamp(2,time)
              powerdata.zipWithIndex.foreach {case (data,dataid) =>
                statementMap(date).setDouble(dataid+3,formatDouble(data=data))}
              statementMap(format.format(time)).addBatch()
              if (index != 0 && index % 40000 == 0) {
                statementMap(date).executeBatch()
                conn.commit()
              }
            }
          }
          statementMap.foreach {case (_,statement) =>
            statement.executeBatch()
            statement.close()
          }

        conn.commit()
        conn.setAutoCommit(true)
        ConnectionPool.returnConnection(conn)
      }
  }

  /**
    * 根据配变mpid的最大最小值，从配变状态信息表tableName中读取配变的最新状态
    * @param conn 数据库连接
    * @param minMpid  mpid的最小值
    * @param maxMpid  mpid的最大值
    * @param tableName  配变最新状态表名，默认是transformer_current_status
    * @return
    */
  def readTransformerCurrentStatus(conn: Connection,
                                   minMpid:Long,
                                   maxMpid:Long,
                                   tableName:String = "transformer_current_status")
  :Map[Long, transformerCurrentStatus]={
    val startTime = System.currentTimeMillis()
    //读取当前状态表transformer_crruent_status
    val statement = conn.prepareStatement(s"select * from ${tableName} where mp_id >= ${minMpid} and mp_id <= ${maxMpid}")
    //每次最多拉取70W条记录
    statement.setFetchSize(100000)
    val rs = statement.executeQuery()

    //获取当前状态数据集
    var dataMap = scala.collection.mutable.Map[Long, transformerCurrentStatus]()
    while (rs.next()){
      val mp_id =rs.getLong(1)
      val time =rs.getTimestamp(2)
      val startArray = Array(rs.getTimestamp(3),rs.getTimestamp(4),
        rs.getTimestamp(5),rs.getTimestamp(6),
        rs.getTimestamp(7),rs.getTimestamp(8),
        rs.getTimestamp(9))
      val durationArray= Array(rs.getLong(10),rs.getLong(11), rs.getLong(12),
        rs.getLong(13),rs.getLong(14),rs.getLong(15),
        rs.getLong(16))
      val status = rs.getByte(17)
      val overLoadstatus = rs.getByte(18)
      dataMap += (mp_id -> transformerCurrentStatus(mp_id,time,startArray,durationArray,status,overLoadstatus))
    }

    statement.close()
    rs.close()
    val endTime = System.currentTimeMillis()
    println("readTransformerCurrentStatus拉取oracle transformer_current_status表的时间："+(endTime-startTime)+" ms")
    dataMap.toMap
  }

  /**
    * 计算配变的状态
    * @param dataList 所需分析的数据集
    * @param loadRateArray  判断重过载的百分比数组
    * @param transformerStatusMap 配变当前状态Map
    * @param transExcStaList  记录需要写入重过载表中的数据
    * @param transNeedupdateList
    * @return
    */
  def calculateTransformerStatus(dataList: List[(Long,List[(Timestamp,Array[Double])])],
                                 loadRateArray: Array[Double],
                                 transformerStatusMap: Map[Long, transformerCurrentStatus],
                                 transExcStaList: ListBuffer[transformerExceedStatus],
                                 transNeedUpdateList:ListBuffer[transformerCurrentStatus])
  :List[(Boolean,ListBuffer[transformerCurrentStatus])]={
    dataList.map{case (mpid,dataSet)=>
      //获取Mpid对应的当前状态，如果当前状态不存在则设置为初始零状态
      //newMpid判断该mpid是否为新增mpid
      val newMpid:Boolean = !transformerStatusMap.contains(mpid)
      val last_transformerCurrentStatus:transformerCurrentStatus = transformerStatusMap.getOrElse(mpid,new transformerCurrentStatus)
      var last_Timestamp = last_transformerCurrentStatus.time
      var last_StartArray = last_transformerCurrentStatus.startArray
      var last_DurationArray = last_transformerCurrentStatus.durationArray
      var last_Status = last_transformerCurrentStatus.status
      var last_OverLoadstatus = last_transformerCurrentStatus.overLoadstatus
      var last_loadRate = last_transformerCurrentStatus.loadRate

      //当前配变时间状态列表
      var transCurStaList =new ListBuffer[transformerCurrentStatus]()

      //对收集的数据进行时间参数计算
      val iter = dataSet.iterator
      //上一条记录的时间戳，初始为配变最新时刻记录的时间戳
      var lastTime:Timestamp = last_Timestamp
      while (iter.hasNext){
        //初始化当前记录状态
        val (current_Timestamp,powerArr) = iter.next()
        val loadRate = powerArr(0)
        val current_StartArray = Array(new Timestamp(1000), new Timestamp(1000), new Timestamp(1000), new Timestamp(1000), new Timestamp(1000), new Timestamp(1000), new Timestamp(1000))
        val current_DurationArray:Array[Long] = Array(0,0,0,0,0,0,0)
        var current_Status:Byte = 0
        var current_OverLoadstatus:Byte = 0

        //如果是迟到数据则不进入以下计算，直接把status=4，所有状态设置为0
        if(current_Timestamp.getTime <= last_Timestamp.getTime) transCurStaList += transformerCurrentStatus(mp_id=mpid,time=current_Timestamp,status=4)
        else{
          for (i<- 0 until 7){
            //负载率大于指定值，且不为异常值
            if(loadRate > loadRateArray(i) && loadRate != 9999.99){
              //上一个状态起始时刻为零(1s)，或者上一个状态起始时刻不为零且超时数据（大于20Min）,重新计算起始时间参数
              val deltaTime:Long = current_Timestamp.getTime - lastTime.getTime
              if(last_StartArray(i).getTime == 1000 ||  ((last_StartArray(i).getTime != 1000) && (deltaTime > 20*60*1000))) {
                //若相邻两个点的时间小于20min且上一个点的负载率小于等于临界负载率，则通过两点连成直线计算起始时刻，
                //否则直接把当前时刻作为起始时刻
                if(deltaTime <= 20*60*1000 && last_loadRate <= loadRateArray(i)){
                  val startTime:Long = (1.0*(current_Timestamp.getTime - lastTime.getTime)/(loadRate - last_loadRate) * (loadRateArray(i)-last_loadRate)).toLong + lastTime.getTime
                  current_StartArray(i) = new Timestamp(startTime)
                }else current_StartArray(i) = current_Timestamp
              } else current_StartArray(i) = last_StartArray(i)
              //持续时间转为分钟
              current_DurationArray(i)=(current_Timestamp.getTime - current_StartArray(i).getTime)/1000/60
            }
          }
          //判断当前状态
          //如果为过载
          if(current_DurationArray(1)>=30){
            //把当前状态设置为过载
            current_Status = 2
            //判断是否为长时过载
            if(current_DurationArray(6) >= 30 || current_DurationArray(5) >=60 || current_DurationArray(4) >= 120 || current_DurationArray(3) >= 240 || current_DurationArray(2) >= 480) current_OverLoadstatus = 2
            else {if(last_OverLoadstatus == 2)current_OverLoadstatus = 3 else current_OverLoadstatus = 1}
            //第一次变为过载
            if(last_Status != 2){
              //last_Status=1，配变由重载变为过载，把重载写入确定重过载表中
              if(last_Status == 1){transExcStaList += transformerExceedStatus(mpid,last_Status,last_OverLoadstatus, last_StartArray(0), current_StartArray(1),
                (current_StartArray(1).getTime-last_StartArray(0).getTime)/1000/60)
              }
              //如果过载起始时刻与第一次变为过载状态的当前时刻的时间差不为15min的整数倍，则插入过载起始时刻记录
              if((current_Timestamp.getTime-current_StartArray(1).getTime)%(15*60*1000) != 0){
                var heavyLoadDuration = 0L
                if(last_Status == 1){heavyLoadDuration = (current_StartArray(1).getTime-last_StartArray(0).getTime)/1000/60}
                transCurStaList += transformerCurrentStatus(mpid,current_StartArray(1),
                Array(new Timestamp(1000),
                  current_StartArray(1), new Timestamp(1000),
                  new Timestamp(1000), new Timestamp(1000),
                  new Timestamp(1000), new Timestamp(1000)),
                Array(heavyLoadDuration,0,0,0,0,0,0),
                current_Status,current_OverLoadstatus,
                loadRate = loadRateArray(1))
              }
              //把本批中处于同一次过载的时刻状态的status全部设置为2
              if(transCurStaList.exists(transState => transState.startArray(1) == current_StartArray(1))){
                transCurStaList.filter(transState => transState.startArray(1) == current_StartArray(1))
                  .foreach{transState=>
                    transState.status = current_Status
                    transState.overLoadstatus = current_OverLoadstatus
                  }
              }
              //把需要更新状态的记录放入transNeedUpdateList
              transNeedUpdateList += transformerCurrentStatus(mpid,current_Timestamp,current_StartArray.clone(),current_DurationArray.clone(),current_Status,current_OverLoadstatus)

            }

            //过载时把重载的状态数据清空
            current_StartArray(0)=new Timestamp(1000)
            current_DurationArray(0) = 0
          }else{//非过载状态
            //若last_Status=2，证明上一状态为过载，由过载变为其他状态写入配变确定状态表
            if(last_Status == 2){
              //如果和上一个时刻的时间间隔小于20min且1.0在两个负载率之间，则认为这期间的配变负载率是线性变化的
              //则过载的结束时刻为负载率等于1.0的时刻，否则为上一时刻
              if((current_Timestamp.getTime-lastTime.getTime) <= 20*60*1000 && (loadRateArray(1)< last_loadRate && loadRate <= loadRateArray(1))){
                val overloadEndTime = (1.0*(current_Timestamp.getTime - lastTime.getTime)/(loadRate - last_loadRate) * (loadRateArray(1)-last_loadRate)).toLong + lastTime.getTime


                transExcStaList += transformerExceedStatus(mpid,last_Status,last_OverLoadstatus, last_StartArray(1), new Timestamp(overloadEndTime), (overloadEndTime - last_StartArray(1).getTime)/60000)

                //过载结束loadrate=1.0，若计算的结束时刻和本记录时刻、上一记录时刻不相等，插入该时间点并标志为过载
                if(overloadEndTime > current_Timestamp.getTime && overloadEndTime < lastTime.getTime){
                  transCurStaList += transformerCurrentStatus(mpid,new Timestamp(overloadEndTime),
                    Array(new Timestamp(overloadEndTime),
                    current_StartArray(1), new Timestamp(1000),
                    new Timestamp(1000), new Timestamp(1000),
                    new Timestamp(1000), new Timestamp(1000)),
                    Array(0,(overloadEndTime - last_StartArray(1).getTime)/60000,0,0,0,0,0),
                    last_Status,last_OverLoadstatus,loadRateArray(1))
                }
                //如果本记录负载率大于重载临界值且上一过载状态与本次状态之间的时间间隔小于20min，则判断为由过载进入重载
                if(current_StartArray(0).getTime != 1000){
                  //当前状态设置为重载，清空过载标志
                  current_Status = 1
                  //把重载的开始时刻设置为过载的结束时刻
                  current_StartArray(0) = new Timestamp(overloadEndTime)
                  current_OverLoadstatus = 0
                  //重新计算重载持续时间
                  current_DurationArray(0)=(current_Timestamp.getTime - current_StartArray(0).getTime)/1000/60
                }
              }else {
                transExcStaList += transformerExceedStatus(mpid,last_Status,last_OverLoadstatus, last_StartArray(1), lastTime, (lastTime.getTime - last_StartArray(1).getTime)/1000/60)
                if(loadRate == 9999.99) current_Status = 3
              }

              //上一状态非过载，且达到重载条件
            }else if((last_Status ==1 && current_DurationArray(0) >= 15) || current_DurationArray(0) >= 30){
              current_Status = 1
              current_OverLoadstatus = 0

              if(last_Status != 1){
                //如果重载起始时刻与第一次变为重载状态的当前时刻的时间差不为15min的整数倍，则插入过载起始时刻记录
                if((current_Timestamp.getTime-current_StartArray(0).getTime)%(15*60*1000) != 0){
                  transCurStaList += transformerCurrentStatus(mpid,current_StartArray(0),
                    Array(current_StartArray(0),
                      new Timestamp(1000), new Timestamp(1000),
                      new Timestamp(1000), new Timestamp(1000),
                      new Timestamp(1000), new Timestamp(1000)),
                    Array(0,0,0,0,0,0,0),
                    current_Status,current_OverLoadstatus,
                    loadRate = loadRateArray(0))
                }

                //把本批中处于同一次重载的时刻状态的status全部设置为1
                if(transCurStaList.exists(transState => transState.startArray(0) == current_StartArray(0))){
                  transCurStaList.filter(transState => transState.startArray(0) == current_StartArray(0)).foreach(_.status=1)
                }
                //把需要更新状态的记录放入transNeedUpdateList
                transNeedUpdateList += transformerCurrentStatus(mpid,current_Timestamp,current_StartArray.clone(),current_DurationArray.clone(),current_Status,current_OverLoadstatus)
              }
            }else{
              //配变正常或负载率计算异常
              if(loadRate == 9999.99) current_Status = 3 else current_Status = 0
              //若上一状态配变为重载，则把状态写入确认重过载表中
              if(last_Status == 1){
                //如果和上一状态之间的时间小于20min且当前状态为正常，认为这期间的配变负载率是线性变化的
                //则过载的结束时刻为负载率等于0.8的时刻，否则为上一时刻
                if((current_Timestamp.getTime-lastTime.getTime)/1000 <= 20*60 && loadRate != 9999.99){
                  //计算重载的结束时刻
                  val heavyloadEndTime = (1.0*(current_Timestamp.getTime - lastTime.getTime)/(loadRate - last_loadRate) * (loadRateArray(0)-last_loadRate)).toLong + lastTime.getTime
                  transExcStaList += transformerExceedStatus(mpid,last_Status,last_OverLoadstatus, last_StartArray(0), new Timestamp(heavyloadEndTime),
                    (heavyloadEndTime-last_StartArray(0).getTime)/1000/60)

                  //重载结束loadrate=0.8，若计算的结束时刻和本记录时刻、上一记录时刻不相等,插入该时间点并标志为正常状态
                  if(heavyloadEndTime != current_Timestamp.getTime && heavyloadEndTime != lastTime.getTime){
                    transCurStaList += transformerCurrentStatus(mp_id=mpid,time=new Timestamp(heavyloadEndTime),
                      status=1,overLoadstatus=current_OverLoadstatus,loadRate=loadRateArray(0))
                  }
                }else{
                  transExcStaList += transformerExceedStatus(mpid,last_Status,last_OverLoadstatus, last_StartArray(0), lastTime,
                    (lastTime.getTime-last_StartArray(0).getTime)/1000/60)
                }
              }
            }
          }
          transCurStaList += transformerCurrentStatus(mpid,current_Timestamp,current_StartArray.clone(),current_DurationArray.clone(),current_Status,current_OverLoadstatus,loadRate)
        }
        //把上一状态更新为本次记录
        lastTime = current_Timestamp
        last_Timestamp = current_Timestamp
        last_StartArray = current_StartArray.clone()
        last_DurationArray = current_DurationArray.clone()
        last_Status = current_Status
        last_OverLoadstatus = current_OverLoadstatus
        last_loadRate = loadRate
      }
      (newMpid,transCurStaList.sortBy(transCurrStatus=>transCurrStatus.time.getTime))
    }
  }

  /**
    * 把确认的配变重过载写入transformer_exceed_status表中
    * @param conn 数据库连接
    * @param transExcStaList  记录需要写入重过载表中的数据
    */
  def writeTransformerExceedStatus(conn: Connection,
                                   transExcStaList: ListBuffer[transformerExceedStatus],
                                   dateArrBr: Broadcast[Array[String]]):Unit={
    conn.setAutoCommit(false)
    //根据日期创建不同的写入表语句
    val statementMap =dateArrBr.value.map{date=>
      val statement = conn.prepareStatement(s"insert into transformer_exceed_status_$date values (?, ?, ?, ?, ?, ?)")
      (date,statement)
    }.toMap
    transExcStaList.zipWithIndex.foreach {case (exceedStatusRecord,index) =>
      val format =new SimpleDateFormat("yyyyMMdd")
      val date = format.format(exceedStatusRecord.ts)
      if(!"19700101".equals(date)){
        val statement = statementMap(date)
        statement.setLong(1,exceedStatusRecord.mp_id)
        statement.setByte(2,exceedStatusRecord.status)
        statement.setByte(3,exceedStatusRecord.overLoadstatus)
        statement.setTimestamp(4,exceedStatusRecord.ts)
        statement.setTimestamp(5,exceedStatusRecord.te)
        statement.setLong(6,exceedStatusRecord.td)
        statement.addBatch()
        if (index != 0 && index % 40000 == 0) {
          statement.executeBatch()
          conn.commit()
        }
      }
    }
    statementMap.foreach {case (_,statement) =>
      statement.executeBatch()
      statement.close()
    }
    conn.commit()
  }

  /**
    *  把配变最新状态写入库表 transformer_current_status
    * @param conn 数据库连接
    * @param currentStatusList  配变最新状态集合
    */
  def writeTransformerCurrentStatus(conn:Connection,
                                    currentStatusList:Iterator[(Boolean,transformerCurrentStatus)]):Unit ={
    val updateSQL =
      """
        |update transformer_current_status set
        |time = ?,
        |ths = ?,tos = ?,tos1 = ?,tos2 = ?,tos3 = ?,tos4 = ?,tos5 = ?,
        |thd = ?,tod = ?,tod1 = ?,tod2 = ?,tod3 = ?,tod4 = ?,tod5 = ?,
        |status = ?,overLoadstatus = ?,
        |loadRate = ?
        |where mp_id = ?
      """.stripMargin
    val paramMark:String = Range(0,19).map(_=>"?").mkString(",")
    val insStaStatement = conn.prepareStatement(s"insert into transformer_current_status values (${paramMark})")
    val updateStatement = conn.prepareStatement(updateSQL)

    //把最新的状态记录取出来写入到当前状态表transformer_crruent_status
    currentStatusList.zipWithIndex.foreach {case ((newMpid,lastTranStatus), index)=>
        if (newMpid) {
          insStaStatement.setLong(1, lastTranStatus.mp_id)
          insStaStatement.setTimestamp(2, lastTranStatus.time)
          (3 to 9).foreach { num =>
            insStaStatement.setTimestamp(num, lastTranStatus.startArray(num - 3))
            insStaStatement.setLong(num + 7, lastTranStatus.durationArray(num - 3))
          }
          insStaStatement.setByte(17, lastTranStatus.status)
          insStaStatement.setByte(18, lastTranStatus.overLoadstatus)
          insStaStatement.setDouble(19, formatDouble(data = lastTranStatus.loadRate))
          insStaStatement.addBatch()
        } else {
          updateStatement.setDouble(18, lastTranStatus.loadRate)
          updateStatement.setLong(19, lastTranStatus.mp_id)
          updateStatement.setTimestamp(1, lastTranStatus.time)
          (2 to 8).foreach { num =>
            updateStatement.setTimestamp(num, lastTranStatus.startArray(num - 2))
            updateStatement.setLong(num + 7, lastTranStatus.durationArray(num - 2))
          }
          updateStatement.setByte(16, lastTranStatus.status)
          updateStatement.setByte(17, lastTranStatus.overLoadstatus)
          updateStatement.addBatch()
        }

      try {
        if (index != 0 && index % 40000 == 0) {
          insStaStatement.executeBatch()
          updateStatement.executeBatch()
          conn.commit()
        }
      }catch {
        case e:Exception => println(e.getMessage+" "+"lastTranStatus.mp_id:"+lastTranStatus.mp_id+
          "lastTranStatus.time:"+lastTranStatus.time+
          "lastTranStatus.loadRate:"+lastTranStatus.loadRate+
          "lastTranStatus.status:"+lastTranStatus.status+
          "lastTranStatus.overLoadstatus:"+lastTranStatus.overLoadstatus+
          "lastTranStatus.startArray:"+lastTranStatus.startArray.mkString(",")+
          "lastTranStatus.durationArray:"+lastTranStatus.durationArray.mkString(","))
      }
    }
    try{
      insStaStatement.executeBatch()
      updateStatement.executeBatch()
    }catch {
      case e:Exception => println("writeTransformerCurrentStatus写到oracle出错!")
    }finally {
      insStaStatement.close()
      updateStatement.close()
      conn.commit()
    }

  }

  /**
    * 把本批次所有的配变状态写入数据库表transformer_ins_status_day
    * @param conn 数据库连接
    * @param currentStatusList 配变本批次所有状态集合
    */
  def writeTransformerInsStatusDay(conn:Connection,
                                   currentStatusList:Iterator[(Boolean,ListBuffer[transformerCurrentStatus])],
                                   dateArrBr: Broadcast[Array[String]]):Unit = {
    val statementMap =dateArrBr.value.map(date=>{
      val paramMark:String = Range(0,19).map(_=>"?").mkString(",")
      val statement = conn.prepareStatement(s"insert into transformer_ins_status_day_$date values (${paramMark})")
      (date,statement)
    }).toMap

    currentStatusList.flatMap {case(_,eleList) => eleList}.zipWithIndex.foreach {case(transCurStatus,index)=>
      val format =new SimpleDateFormat("yyyyMMdd")
      val date = format.format(transCurStatus.time)
      if(statementMap.get(date).nonEmpty){
        statementMap(date).setLong(1,transCurStatus.mp_id)
        statementMap(date).setTimestamp(2,transCurStatus.time)
        (3 to 9).foreach{num =>
          statementMap(date).setTimestamp(num,transCurStatus.startArray(num-3))
          statementMap(date).setLong(num+7,transCurStatus.durationArray(num-3))
        }
        statementMap(date).setByte(17,transCurStatus.status)
        statementMap(date).setByte(18,transCurStatus.overLoadstatus)
        statementMap(date).setDouble(19,formatDouble(data=transCurStatus.loadRate))
        statementMap(date).addBatch()
        if (index != 0 && index % 40000 == 0) {
          statementMap(date).executeBatch()
          conn.commit()
        }
      }
    }

    statementMap.foreach {case (_,statement) =>
      statement.executeBatch()
      statement.close()
    }
    conn.commit()
  }

  /**
    * 计算配变的最大视在功率和最大负载率
    * @param powerData 配变的功率测量数组
    * @param bl 配变的倍率
    * @param capacity 配变容量
    * @return Array[Double]
    */
  def calculateMaxpowerAndRate(powerData:Array[Double],bl:Double,capacity:Double): Array[Double]={
      //求出功率对应的一次值
      val power=(for(i<-0 until powerData.length-4)yield {if(powerData(i)!=9999.99){
        val v=powerData(i)* bl
        if(v >= 9999.99)9999.99
        else v
      } else powerData(i)})
        .toArray
      //功率因数，直接用计量推送过来的功率因数
      val powerFactor=(for(i<-powerData.length-4 until powerData.length)yield powerData(i)).toArray

      val aloneSum=power(1)+power(2)+power(3)+power(5)+power(6)+power(7)
      val totSum=power(0)+power(4)
      //求出最大视在功率
      //取计量推送过来的总视在功率与三项视在功率之和两个值的最大值，
      //如果两者之间有异常值9999.99，则含有异常值9999.99的值直接抛弃
      val powerMax:Double =
      (aloneSum,totSum) match{
        case (aloneSum,totSum) if aloneSum<9999.99 && totSum<9999.99 =>math.max(
          math.sqrt(power(0)*power(0) +power(4)*power(4)),
          math.sqrt((power(1)+power(2)+power(3))*(power(1)+power(2)+power(3))
            +(power(5)+power(6)+power(7))*(power(5)+power(6)+power(7))))
        case (aloneSum,totSum) if aloneSum>=9999.99 && totSum<9999.99 => math.sqrt(power(0)*power(0)+power(4)*power(4))
        case (aloneSum,totSum) if aloneSum<9999.99 && totSum>=9999.99 => math.sqrt((power(1)+power(2)+power(3))*
          (power(1)+power(2)+power(3))
          +(power(5)+power(6)+power(7))*(power(5)+power(6)+power(7)))
        case _=> 9999.99
      }
      //求出负载率
      //如果额定容量为0或者最大功率为异常值，则负载率为异常值9999.99
      val loadRate:Double= if(capacity ==0 || powerMax == 9999.99)9999.99 else formatDouble(powerMax / capacity)

      //返回Array(负载率，最大视在功率，总有功功率,A相有功功率,B相有功功率,C相有功功率,
      // 总无功功率,A相无功功率,B相无功功率,C相无功功率,
      // 视在功率,A相视在功率,B相视在功率,C相视在功率,总功率因数,A相功率因数,B相功率因数,C相功率因数)
      Array.concat(Array(loadRate,powerMax),power,powerFactor)
  }

  def dataCleaning(kafkaRDD:RDD[ConsumerRecord[String,String]])
  :RDD[(Long,List[(Timestamp,Array[Double])])]={
    //过滤掉不符合p匹配模式的行
    val cleanedRDD1 = kafkaRDD.filter{kafkaLine=>
      val p = Range(0,17).map{_=>"[^\\|]*\\|\\|"}.mkString("") + "[^\\|]*"
      Pattern.matches(p,kafkaLine.value())
    }.map { kafkaLine =>
      //去掉每一行的BOM并以“||”分割每一行
      val lineArr = kafkaLine.value().replaceAll("^[^0-9]*", "")
        .split("\\|\\|", -1).map(_.trim)
      //正则匹配是否为数字，如果不是则设置为9999.99
      val powerData = (for (i <- 2 until lineArr.length) yield {
        val isNumber = Pattern.matches("^(\\-|\\+)?\\d+(\\.\\d+)?$", lineArr(i))
        if (!isNumber) 9999.99
        else formatDouble(lineArr(i).toDouble)
      }).toArray
      //(mpid,(time,data))
      (lineArr(0), (lineArr(1), powerData))
    }.filter { case (mpid, (time, _)) =>
      try{
        //如果mpid能正常转为Long类型，且eventTime能正常转为Timestamp;
        // 且eventTime小于当前系统时间大于当前系统时间之前一天的时间内
        //则不过滤
        val a = mpid.toLong
        val b = Timestamp.valueOf(time)
        val currentTimeMillis = System.currentTimeMillis()
        if((currentTimeMillis - 86400000) <= b.getTime && b.getTime <= currentTimeMillis) true else false
      }catch{
        case numE: NumberFormatException => false
        case iE:IllegalArgumentException => false
      }
    } //过滤mpid和time为非法值
    //对kafkaRDD重新分为10个partitions，然后再清洗
    cleanedRDD1.groupByKey(new RangePartitioner(20,cleanedRDD1)) //把mpid相同的行合并
      .map { case (mpid, iter) => //把时间String转为TimeStamp并根据时间进行升序排序
      val value = iter.map { case (time, data) =>
        (Timestamp.valueOf(time), data)
      }.toList.distinct.sortBy(_._1.getTime)
      (mpid.toLong, value)
    }
  }

  /**
    * 更新transformer_ins_status_day表中的状态
    * @param conn 数据库连接
    * @param transNeedUpdateList 需要更新的状态时刻
    * @param dateArrBr 日期广播变量
    */
  def updatestartStatus(conn:Connection,
                        transNeedUpdateList:ListBuffer[transformerCurrentStatus],
                        dateArrBr: Broadcast[Array[String]])={
    val statementMap =(1 to 2).flatMap(num=>dateArrBr.value.map(date=>(date,num))).map{case(date,status)=>
      val statement =
        if(status == 1)conn.prepareStatement(s"update transformer_ins_status_day_$date t set t.status = ?, t.overLoadstatus = ? where mp_id = ? and t.ths = ? and t.time < ?")
        else conn.prepareStatement(s"update transformer_ins_status_day_$date t set t.status = ?, t.overLoadstatus = ? where mp_id = ? and t.tos = ? and t.time < ?")
      ((date,status),statement)
    }.toMap
    val format =new SimpleDateFormat("yyyyMMdd")
    transNeedUpdateList.zipWithIndex.foreach {case(transCurStatus,index)=>
      val date = format.format(transCurStatus.time)
      val status:Byte = transCurStatus.status
      statementMap((date,status)).setByte(1,transCurStatus.status)
      statementMap((date,status)).setByte(2,transCurStatus.overLoadstatus)
      statementMap((date,status)).setLong(3,transCurStatus.mp_id)
      statementMap((date,status)).setTimestamp(5,transCurStatus.time)
      status match {
        case 1 => statementMap((date,status)).setTimestamp(4,transCurStatus.startArray(0))
        case 2 => statementMap((date,status)).setTimestamp(4,transCurStatus.startArray(1))
      }
      statementMap((date,status)).addBatch()
      if (index != 0 && index % 40000 == 0) {
        statementMap((date,status)).executeBatch()
        conn.commit()
      }
    }
    statementMap.foreach {case (_,statement) =>
      statement.executeBatch()
      statement.close()
    }
    conn.commit()
  }

  /**
    * 用于格式化写入到数据库的Double类型值
    * @param data 输入Double类型数字
    * @param maxValue 最大值
    * @param format 格式化
    * @return 解析后的Double数字
    */
  def formatDouble(data:Double,maxValue:Double=9999.99,format:String="######0.0000"):Double={
    if(Math.abs(data) > maxValue) maxValue
    else{
      val f = new DecimalFormat("0000.0000")
      //保留两位四舍五入
      f.setRoundingMode(RoundingMode.HALF_UP)
      val str:String = f.format(data)
      str.toDouble
    }
  }


}

//Mysql的最小时间戳不支持0,因此设置为1s
case class transformerCurrentStatus(mp_id:Long=0,time:Timestamp = new Timestamp(1000),
                                    startArray:Array[Timestamp] = Array(new Timestamp(1000),
                                      new Timestamp(1000), new Timestamp(1000),
                                      new Timestamp(1000), new Timestamp(1000),
                                      new Timestamp(1000), new Timestamp(1000)),
                                    durationArray:Array[Long] = Array(0,0,0,0,0,0,0),
                                    var status:Byte=0,var overLoadstatus:Byte=0,
                                    loadRate:Double = 9999.99)

case class transformerExceedStatus(mp_id:Long,status:Byte,overLoadstatus:Byte,ts:Timestamp,te:Timestamp,td:Long)







