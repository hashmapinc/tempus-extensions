package com.hashmapinc.tempus

import java.io.FileInputStream
import java.util.Properties

import com.hashmapinc.tempus.util.TempusKuduConstants
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, HasOffsetRanges, KafkaUtils}
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.kudu.spark.kudu._
import scala.util.parsing.json.{JSON, JSONObject}






/**
  * @author Mitesh Rathore
  */

object PutDepthLog {

  val log = Logger.getLogger(PutDataInKudu.getClass)
  val specialKeySet = Map("tempus.tsds"->"tempus.tsds", "tempus.hint"->"tempus.hint", "nameWell"->"nameWell", "nameWellbore"->"nameWellbore", "LogName"->"LogName")

  val groupId = "DEPTH"
  def streamDataFromKafkaToKudu(kafkaUrl: String, topics: Array[String], kuduUrl: String, kuduTableName:String,impalaKuduUrl:String,kuduUser:String,kuduPassword:String, level: String="WARN"): Unit = {
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> kafkaUrl , //kafka,
      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer", //classOf[StringDeserializer],
      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer", //classOf[StringDeserializer],
      "group.id" -> groupId,
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    val sparkConf = new SparkConf().setAppName("PutDataInKudu")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val ssc = new StreamingContext(sc, Seconds(10))
    val con =  TempusKuduConstants.getImpalaConnection(impalaKuduUrl, kuduUser, kuduPassword)
    val fromOffsets= TempusKuduConstants.getLastCommittedOffsets(con,topics(0),groupId)
    val stream = KafkaUtils.createDirectStream[String, String](ssc , PreferConsistent, ConsumerStrategies.Subscribe[String, String](topics, kafkaParams,fromOffsets))

  /*  val values  = stream.map(_.value())
      .filter(_.length>0)                     //Ignore empty lines
      .map(toMap(_)).filter(_.size>0)
      .flatMap(toDepthLog(_))            //Ignore empty records - id for growing objects and nameWell for attributes

*/
    stream
      .transform {
        rdd =>
         val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
          offsetRanges.foreach(offset => {
            println(" ============================= "+offset.topic,offset.partition, offset.fromOffset, offset.untilOffset)
            TempusKuduConstants.saveOffsets(con,topics(0),groupId,offset.untilOffset)
          })
       rdd
      }.map(_.value())
      .filter(_.length>0)                     //Ignore empty lines
      .map(toMap(_)).filter(_.size>0)
      .flatMap(toDepthLog(_)).foreachRDD(rdd =>{
      INFO("before upserting")

     // val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges


      rdd.toDF().write.options(Map("kudu.table" -> kuduTableName,"kudu.master" -> kuduUrl)).mode("append").kudu



      //rdd.toDF().show(false)
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

  def toDepthLog(map: Map[String, String]): Array[DepthLog]= {
    val dla: Array[DepthLog] = new Array[DepthLog](map.size-specialKeySet.size)
    var ds = map("tempus.tsds")
    var logName = map("LogName")
    var nameWell = map("nameWell")
    var nameWellbore = map("nameWellbore")

    if (ds.length()<10) {
      ds = "0000000000".substring(0, (10-ds.length()))+ds
    }
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
        dla(i)=DepthLog(nameWell, nameWellbore, logName, mnemonic, ds, map("tempus.tsds").toDouble, value, valuestr)
        i = i+1
      }
    }

    dla
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
    var kuduTableName = "impala::kudu_tempus.depth_log"
    var impalaKuduUrl = ""

    try{
      val prop = new Properties()
      prop.load(new FileInputStream("kudu_witsml.properties"))
      kafkaUrl = prop.getProperty(TempusKuduConstants.KAFKA_URL_PROP)
      kuduConnectionUrl = prop.getProperty(TempusKuduConstants.KUDU_CONNECTION_URL_PROP)
      kuduConnectionUser = prop.getProperty(TempusKuduConstants.KUDU_CONNECTION_USER_PROP)
      kuduConnectionPassword = prop.getProperty(TempusKuduConstants.KUDU_CONNECTION_PASSWORD_PROP)
      logLevel = prop.getProperty(TempusKuduConstants.LOG_LEVEL)
      topicName = prop.getProperty(TempusKuduConstants.TOPIC_DEPTHLOG_PROP)

      impalaKuduUrl = prop.getProperty(TempusKuduConstants.KUDU_IMPALA_CONNECTION_URL_PROP)

      kuduTableName = "impala::"+prop.getProperty(TempusKuduConstants.KUDU_DEPTHLOG_TABLE)


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
        PutDepthLog.streamDataFromKafkaToKudu(kafkaUrl, Array(topicName), kuduConnectionUrl,kuduTableName,impalaKuduUrl,kuduConnectionUser,kuduConnectionPassword,logLevel)
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
