package com.hashmapinc.tempus

import java.io.FileInputStream
import java.util.Properties

import com.hashmapinc.kafka.KafkaService
import com.hashmapinc.kudu.KuduService
import com.hashmapinc.spark.SparkService
import com.hashmapinc.util.{TempusKuduConstants, TempusUtils}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.kafka010.HasOffsetRanges
import org.apache.kudu.spark.kudu._

/**
  * @author Mitesh Rathore
  */
object PutTimeLog {



  val log = Logger.getLogger(PutTimeLog.getClass)

  val groupId = "TimeLog"

  def processTimeLog(kafkaUrl: String, topics: Array[String], kuduUrl: String, kuduTableName:String,impalaKuduUrl:String,kuduUser:String,kuduPassword:String, groupId:String, timeWindow :String, level: String="WARN"): Unit = {
    val connection =  KuduService.getImpalaConnection(impalaKuduUrl, kuduUser, kuduPassword)

    val spark = SparkService.getSparkSession("PutTimeLog")

    var timeWindowInt = 10
    if(!TempusUtils.isEmpty(timeWindow))
      timeWindowInt = timeWindow.toInt

    val streamContext = SparkService.getStreamContext(spark,timeWindowInt)

    streamContext.sparkContext.setLogLevel(level)

    import spark.implicits._
    val stream = KafkaService.readKafka(kafkaUrl, topics, impalaKuduUrl,kuduUser,kuduPassword, groupId, streamContext)

 stream
      .transform {
        rdd =>
          val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
          offsetRanges.foreach(offset => {
            //This method will save the offset to kudu_tempus.offsetmgr table
            KuduService.saveOffsets(connection,topics(0),groupId,offset.untilOffset)
          })
          rdd
      }.map(_.value())
      .filter(_.length>0)                     //Ignore empty lines
      .map(TempusUtils.toMap(_))
      .filter(_.size>0)
      .flatMap(toKuduTimeLog(_))

    .foreachRDD(rdd =>
      {
      TempusUtils.INFO("before Timelog upserting")

      rdd.toDF().write.options(Map("kudu.table" -> kuduTableName,"kudu.master" -> kuduUrl)).mode("append").kudu
      TempusUtils.INFO("after Timelog upserting")
    }

    )

    streamContext.start()
    streamContext.awaitTermination()
  }


  def toKuduTimeLog(map: Map[String, String]): Array[TimeLog]= {
    val timeLogData: Array[TimeLog] = new Array[TimeLog](map.size-TempusKuduConstants.specialKeySet.size)
    var logTime  = map("tempus.tsds")
    var logName = map("LogName")
    var nameWell = map("nameWell")
    var nameWellbore = map("nameWellbore")

    logTime = TempusUtils.getFormattedTime(logTime)

    var keyIter = map.keys.toIterator
    var i=0
    while (keyIter.hasNext) {
      val key = keyIter.next()
      if (!TempusUtils.isSpecialKey(key)) {

        val mnemonic = key
        val valuestr = map.getOrElse(key, null)
        var value=0.0
        try {
          value = valuestr.toDouble
        } catch {
          case  exp : Exception => value = 0.0
        }
        timeLogData(i)=TimeLog(nameWell, nameWellbore, logName, mnemonic, logTime, value, valuestr)
        i = i+1
      }
    }

    timeLogData
  }


  def main(args: Array[String]): Unit={

    var kafkaUrl = ""
    var kuduConnectionUrl = ""
    var kuduConnectionUser = ""
    var kuduConnectionPassword = ""
    var topicName = ""
    var logLevel = ""
    var kuduTableName = "impala::kudu_tempus.time_log"
    var impalaKuduUrl = ""
    var groupId = ""
    var timeWindow = ""


    try{
      val prop = new Properties()
      prop.load(new FileInputStream("kudu_witsml.properties"))

      kafkaUrl = prop.getProperty(TempusKuduConstants.KAFKA_URL_PROP)
      kuduConnectionUrl = prop.getProperty(TempusKuduConstants.KUDU_CONNECTION_URL_PROP)
      kuduConnectionUser = prop.getProperty(TempusKuduConstants.KUDU_CONNECTION_USER_PROP)
      kuduConnectionPassword = prop.getProperty(TempusKuduConstants.KUDU_CONNECTION_PASSWORD_PROP)
      logLevel = prop.getProperty(TempusKuduConstants.LOG_LEVEL)
      topicName = prop.getProperty(TempusKuduConstants.TOPIC_TIMELOG_PROP)
      kuduTableName = "impala::"+prop.getProperty(TempusKuduConstants.KUDU_TIMELOG_TABLE)
      impalaKuduUrl = prop.getProperty(TempusKuduConstants.KUDU_IMPALA_CONNECTION_URL_PROP)
      groupId =  prop.getProperty(TempusKuduConstants.TIMELOG_KAFKA_GROUP)
      timeWindow =  prop.getProperty(TempusKuduConstants.TIMELOG_TIME_WINDOW)


      log.info(" kafkaUrl  --- >> "+kafkaUrl)
      log.info(" topicName  --- >> "+topicName)
      log.info(" kuduConnectionUrl --- >> "+kuduConnectionUrl)
      log.info(" kuduConnectionUser --- >> "+kuduConnectionUser)
      log.info(" kuduConnectionPassword --- >> "+kuduConnectionPassword)
      log.info(" topicName  --- >> "+topicName)
      log.info(" kuduTableName  --- >> "+kuduTableName)
      log.info(" impalaKuduUrl --- >> "+impalaKuduUrl)
      log.info(" groupId --- >> "+groupId)
      log.info(" timeWindow --- >> "+timeWindow)

      if(TempusUtils.isEmpty(kafkaUrl) || TempusUtils.isEmpty(topicName)  || TempusUtils.isEmpty(kuduConnectionUrl) || TempusUtils.isEmpty(kuduConnectionUser)  || TempusUtils.isEmpty(kuduConnectionPassword)){
        log.info("  <<<--- kudu_witsml.properties file should be presented at classpath location with following properties " +
          "kudu.db.url=<HOST_IP>:<PORT>/<DATABASE_SCHEMA>\nkudu.db.user=demo\nkudu.db.password=demo\nkafka.url=kafka:9092\n" +
          "topic.witsml.timelog=well-log-ts-data --- >> ")
      }
      else{
        PutTimeLog.processTimeLog(kafkaUrl, Array(topicName), kuduConnectionUrl,kuduTableName,impalaKuduUrl,kuduConnectionUser,kuduConnectionPassword,groupId,timeWindow,logLevel)
      }


    }catch{
      case  exp : Exception => exp.printStackTrace()
    }

  }


}
