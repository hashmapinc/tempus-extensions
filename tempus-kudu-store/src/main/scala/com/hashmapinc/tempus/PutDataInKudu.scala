package com.hashmapinc.tempus

import java.io.FileInputStream
import java.util.Properties

import com.hashmapinc.tempus.util.{TempusKuduConstants, TempusUtils}
import org.apache.commons.lang3.StringUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.kudu.spark.kudu._

import scala.tools.scalap.scalax.util.StringUtil
import scala.util.parsing.json.{JSON, JSONObject}

/**
  * @author Mitesh Rathore
  */



object PutDataInKudu {


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




      val ssc = new StreamingContext(sc, Seconds(60))

      val stream = KafkaUtils
        .createDirectStream[String, String](ssc, PreferConsistent, Subscribe[String, String](topics, kafkaParams))



      val values  = stream.map(_.value())
            .filter(_.length>0)                     //Ignore empty lines
            .map(toMap(_)).filter(_.size>0)
            .flatMap(toDepthLog(_))            //Ignore empty records - id for growing objects and nameWell for attributes




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

  def toTrajectory(map: Map[String, String]): Trajectory= {

    var nameWell = map("nameWell")
    var nameWellbore = map("nameWellbore")
    var nameTrajectory = map("nameTrajectory")
    var nameTrajectoryStn = map("nameTrajectoryStn")
    var aziVertSectUom = map("aziVertSectUom")
    var mdUom = map("mdUom")

    var tvdUom = map("tvdUom")
    var inclUom = map("inclUom")
    var aziUom = map("aziUom")
    var dispNsUom = map("dispNsUom")

    var dispEwUom = map("dispEwUom")
    var vertSectUom = map("vertSectUom")
    var dlsUom = map("dlsUom")




    var aziVertSectValue  = map("aziVertSectValue")
    var aziVertSectValueDouble = 0.0
    if(!aziVertSectValue.isEmpty){
        aziVertSectValueDouble = aziVertSectValue.toDouble
    }

    var mdValue = map("mdValue")
    var mdValueDouble = 0.0
    if(!mdValue.isEmpty){
      mdValueDouble = mdValue.toDouble
    }


    var dispNsVertSecOrigValue = map("dispNsVertSecOrigValue")
    var dispNsVertSecOrigValueDouble = 0.0
    if(!dispNsVertSecOrigValue.isEmpty){
      dispNsVertSecOrigValueDouble = dispNsVertSecOrigValue.toDouble
    }


    var dispEwVertSecOrigValue = map("dispEwVertSecOrigValue")
    var dispEwVertSecOrigValueDouble = 0.0
    if(!dispEwVertSecOrigValue.isEmpty){
      dispEwVertSecOrigValueDouble = dispEwVertSecOrigValue.toDouble
    }

    var tvdValue = map("tvdValue")
    var tvdValueDouble = 0.0
    if(!tvdValue.isEmpty){
      tvdValueDouble = tvdValue.toDouble
    }

    var inclValue = map("inclValue")
    var inclValueDouble = 0.0
    if(!inclValue.isEmpty){
      inclValueDouble = inclValue.toDouble
    }


    var aziValue = map("aziValue")
    var aziValueDouble = 0.0
    if(!aziValue.isEmpty){
      aziValueDouble = aziValue.toDouble
    }

    var dispNsValue = map("dispNsValue")
    var dispNsValueDouble = 0.0
    if(!dispNsValue.isEmpty){
      dispNsValueDouble = dispNsValue.toDouble
    }



    var dispEwValue = map("dispEwValue")
    var dispEwValueDouble = 0.0
    if(!dispEwValue.isEmpty){
      dispEwValueDouble = dispEwValue.toDouble
    }

    var dlsValue = map("dlsValue")
    var dlsValueDouble = 0.0
    if(!dlsValue.isEmpty){
      dlsValueDouble = dlsValue.toDouble
    }

    var vertSectValue = map("vertSectValue")
    var vertSectValueDouble = 0.0
    if(!vertSectValue.isEmpty){
      vertSectValueDouble = vertSectValue.toDouble
    }


    var dispEwVertSecOrigUom = map("dispEwVertSecOrigUom")
    var aziRef = map("aziRef")
    var cmnDataDtimCreation = map("cmnDataDtimCreation")
    var cmnDataDtimLstChange = map("cmnDataDtimLstChange")
    var trajectoryStnType = map("trajectoryStnType")

    var dtimStn = map("dtimStn")


    Trajectory(nameWell, nameWellbore, nameTrajectory, nameTrajectoryStn, aziVertSectValueDouble,aziVertSectUom,
      dispEwVertSecOrigValueDouble,dispEwVertSecOrigUom,dispEwVertSecOrigValueDouble,dispEwVertSecOrigUom,
      aziRef,cmnDataDtimCreation,cmnDataDtimLstChange,trajectoryStnType,mdValueDouble,mdUom,tvdValueDouble,tvdUom,
      inclValueDouble,inclUom,aziValueDouble,aziUom,dispNsValueDouble,dispNsUom,dispEwValueDouble,dispEwUom,
      vertSectValueDouble,vertSectUom,dlsValueDouble,dlsUom,dtimStn,TempusUtils.getCurrentTime)



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
      var objectType = args(0)

      try{
        val prop = new Properties()
        prop.load(new FileInputStream("kudu_witsml.properties"))

        kafkaUrl = prop.getProperty(TempusKuduConstants.KAFKA_URL_PROP)
        kuduConnectionUrl = prop.getProperty(TempusKuduConstants.KUDU_CONNECTION_URL_PROP)
        kuduConnectionUser = prop.getProperty(TempusKuduConstants.KUDU_CONNECTION_USER_PROP)
        kuduConnectionPassword = prop.getProperty(TempusKuduConstants.KUDU_CONNECTION_PASSWORD_PROP)

        logLevel = prop.getProperty(TempusKuduConstants.LOG_LEVEL)

        if(TempusKuduConstants.DEPTHLOG_OBJECTTYPE.equalsIgnoreCase(objectType)){
          topicName = prop.getProperty(TempusKuduConstants.TOPIC_DEPTHLOG_PROP)
          kuduTableName = "impala::kudu_tempus.depth_log"
        }else  if(TempusKuduConstants.TIMELOG_OBJECTTYPE.equalsIgnoreCase(objectType)){
          topicName = prop.getProperty(TempusKuduConstants.TOPIC_TIMELOG_PROP)
          kuduTableName = "impala::kudu_tempus.time_log"
        }else  if(TempusKuduConstants.MESSAGE_OBJECTTYPE.equalsIgnoreCase(objectType)){
          topicName = prop.getProperty(TempusKuduConstants.TOPIC_MESSAGE_PROP)
          kuduTableName = "impala::kudu_tempus.time_log"
        }else  if(TempusKuduConstants.TRAJECTORY_OBJECTTYPE.equalsIgnoreCase(objectType)){
          topicName = prop.getProperty(TempusKuduConstants.TOPIC_TRAJECTORY_PROP)
          kuduTableName = "impala::kudu_tempus.trajectory"
        }else  if(TempusKuduConstants.ATTRIBUTE_OBJECTTYPE.equalsIgnoreCase(objectType)){
          topicName = prop.getProperty(TempusKuduConstants.TOPIC_ATTRIBUTE_PROP)
          kuduTableName = "impala::kudu_tempus.well"
        }

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

          PutDataInKudu.streamDataFromKafkaToKudu(kafkaUrl, Array(topicName), kuduConnectionUrl,kuduTableName,logLevel)

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
