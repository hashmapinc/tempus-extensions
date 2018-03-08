package com.hashmapinc.tempus

import java.io.FileInputStream
import java.text.SimpleDateFormat
import java.util.{Calendar, Properties}

import com.hashmapinc.tempus.util.TempusKuduConstants
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent

import scala.util.parsing.json.{JSON, JSONObject}
import org.apache.kudu.spark.kudu._

/**
  * @author Mitesh Rathore
  */
object PutTimeLog {



  val log = Logger.getLogger(PutDataInKudu.getClass)
  val specialKeySet = Map("tempus.tsds"->"tempus.tsds", "tempus.hint"->"tempus.hint", "nameWell"->"nameWell", "nameWellbore"->"nameWellbore", "LogName"->"LogName")

  def streamDataFromKafkaToKudu(kafkaUrl: String, topics: Array[String], kuduUrl: String, kuduTableName:String, level: String="WARN"): Unit = {
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> kafkaUrl , //kafka,
      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer", //classOf[StringDeserializer],
      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer", //classOf[StringDeserializer],
      "group.id" -> topics(0),
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    val sparkConf = new SparkConf().setAppName("PutDataInKudu")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._




    val ssc = new StreamingContext(sc, Seconds(1))

    val stream = KafkaUtils
      .createDirectStream[String, String](ssc, PreferConsistent, Subscribe[String, String](topics, kafkaParams))



    val values  = stream.map(_.value())
      .filter(_.length>0)                     //Ignore empty lines
      .map(toMap(_)).filter(_.size>0)
      .flatMap(toTimeLog(_))            //Ignore empty records - id for growing objects and nameWell for attributes




    values.foreachRDD(rdd =>{
      INFO("before upserting")
      rdd.toDF().write.options(Map("kudu.table" -> kuduTableName,"kudu.master" -> kuduUrl)).mode("append").kudu
      rdd.toDF().show(false)
      INFO("after upserting")
    })

    ssc.start()
    ssc.awaitTermination()
  }

  def toMap(record: String): Map[String, String]= {
    var result = JSON.parseRaw(record).getOrElse(null)
    if (result == null) {
      WARN(s"Record could not be parsed as a JSON object: ${record}")
      Map()
    } else {
      var map = result.asInstanceOf[JSONObject].obj.asInstanceOf[Map[String, String]]
      map
    }
  }

  def toTimeLog(map: Map[String, String]): Array[TimeLog]= {
    val timeLogData: Array[TimeLog] = new Array[TimeLog](map.size-specialKeySet.size)
    var logTime  = map("tempus.tsds")
    var logName = map("LogName")
    var nameWell = map("nameWell")
    var nameWellbore = map("nameWellbore")

    logTime = TempusKuduConstants.getFormattedTime(logTime)

    var keyIter = map.keys.toIterator
    var i=0
    while (keyIter.hasNext) {
      val key = keyIter.next()
      if (!isSpecialKey(key)) {

        val mnemonic = key
        val valuestr = map.getOrElse(key, null)
        var value=0.0
        try {
          value = valuestr.toDouble
        } catch {
          case _ => 0.0
        }
        timeLogData(i)=TimeLog(nameWell, nameWellbore, logName, mnemonic, logTime, value, valuestr)
        i = i+1
      }
    }

    timeLogData
  }

  def isSpecialKey(key: String): Boolean= {
    if (specialKeySet.getOrElse(key, null)!=null)
      return true
    return false
  }


  def main(args: Array[String]): Unit={

    var kafkaUrl = ""
    var kuduConnectionUrl = ""
    var kuduConnectionUser = ""
    var kuduConnectionPassword = ""
    var topicName = ""
    var logLevel = ""
    var kuduTableName = "impala::kudu_tempus.time_log"


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


      log.info(" kafkaUrl  --- >> "+kafkaUrl)
      log.info(" topicName  --- >> "+topicName)
      log.info(" kuduConnectionUrl --- >> "+kuduConnectionUrl)
      log.info(" kuduConnectionUser --- >> "+kuduConnectionUser)
      log.info(" kuduConnectionPassword --- >> "+kuduConnectionPassword)

      if(kafkaUrl.isEmpty || topicName.isEmpty || kuduConnectionUrl.isEmpty || kuduConnectionUser.isEmpty || kuduConnectionPassword.isEmpty){
        log.info("  <<<--- kudu_witsml.properties file should be presented at classpath location with following properties " +
          "kudu.db.url=<HOST_IP>:<PORT>/<DATABASE_SCHEMA>\nkudu.db.user=demo\nkudu.db.password=demo\nkafka.url=kafka:9092\n" +
          "topic.witsml.attribute=well-attribute-data --- >> ")
      }
      else{
        PutTimeLog.streamDataFromKafkaToKudu(kafkaUrl, Array(topicName), kuduConnectionUrl,kuduTableName,logLevel)
      }


    }catch{
      case  exp : Exception => exp.printStackTrace()
    }

  }

  def WARN(s: String): Unit={
    if (log.isEnabledFor(Level.WARN)) {
      log.warn(s)
    }
  }

  def INFO(s: String): Unit={
    if (log.isInfoEnabled()) {
      log.info(s)
    }
  }

  def DEBUG(s: String): Unit={
    if (log.isDebugEnabled()) {
      log.debug(s)
    }
  }

  def ERROR(s: String): Unit={
    log.error(s)
  }

}
