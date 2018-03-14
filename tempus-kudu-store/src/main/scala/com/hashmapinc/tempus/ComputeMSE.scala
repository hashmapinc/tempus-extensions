package com.hashmapinc.tempus

import java.io.FileInputStream
import java.util.Properties

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._

import scala.util.parsing.json._
import scala.collection.mutable.HashMap
import com.fasterxml.jackson.databind.ObjectMapper
import com.hashmapinc.tempus.util.TempusKuduConstants

import scala.tools.scalap.scalax.util.StringUtil


object ComputeMSE {
  //val logLevelMap = Map("INFO"->Level.INFO, "WARN"->Level.WARN, "DEBUG"->Level.DEBUG)
  val log = Logger.getLogger(ComputeMSE.getClass)

  var torKey :String = "";
  var wobKey :String = "";
  var rpmKey :String = "";
  var ropKey :String = "";
  var mnemonicName :String = "";


  def streamComputedMSE(kafka: String, topics: Array[String], mqttUrl: String, gatewayToken: String, mqttTopic:String, level: String="WARN"): Unit = {
   // log.setLevel(TempusKuduConstants.logLevelMap(level))
  //  TempusPublisher.setLogLevel(logLevelMap(level))
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> kafka,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> topics(0),
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    val sparkConf = new SparkConf().setAppName("ComputeMSE")
    val ssc = new StreamingContext(sparkConf, Seconds(1))
    ssc.sparkContext.setLogLevel("WARN")

    val stream = KafkaUtils
      .createDirectStream[String, String](ssc, PreferConsistent, Subscribe[String, String](topics, kafkaParams))

    assert((stream != null), ERROR("Kafka stream is not available. Check your Kafka setup."))

    //Stream could be empty but that is perfectly okay
    val values  = stream.map(_.value())
                        .filter(_.length>0)                     //Ignore empty lines
                        .map(toMap(_))
                        .filter(isValidRecord(_))            //Ignore empty records - id for growing objects and nameWell for attributes
                        .map(computeMSE(_))

   // var con : MqttAsyncClient = null
   //   if(con==null){

    //  }

    values.foreachRDD(rdd =>{
      if (!rdd.isEmpty()) {
        rdd.foreachPartition { p =>
          val con = TempusPublisher.connect(mqttUrl, gatewayToken)
          p.foreach(record => TempusPublisher.publishMSE(con, mqttTopic, toTempusData(record)))

        }
      }
    })

    ssc.start()
    ssc.awaitTermination()
   // con.disconnect()
  }

  def toTempusData(record: Map[String, String]): String = {
    val mapper = new ObjectMapper()
    val json = mapper.createObjectNode()
    var ja = json.putArray(record.getOrElse("nameWell", "nameWell"))
    var obj=ja.addObject()
    obj.put("ds", record.getOrElse("tempus.tsds", "0.00"))
    obj=obj.putObject("values")
    val key=record.getOrElse(mnemonicName, "CALC_MSE")
    var mseValue : Double = 0.0
    var keyValue = record.getOrElse(key, "")

   // INFO("  keyValue ===>>> "+keyValue)

    if(!keyValue.isEmpty)
      mseValue = keyValue.toDouble

   // INFO("  mseValue ===>>> "+mseValue)

    obj.put(key, mseValue)
    INFO(s"Publishing: ${mapper.writeValueAsString(json)}")
    mapper.writeValueAsString(json)
  }

  def computeMSE(record: Map[String, String]): Map[String, String]={
    val tor: Double = record.getOrElse(torKey, 1.0).toString.toDouble
    val rpm: Double = record.getOrElse(rpmKey, 1.0).toString.toDouble
    val rop: Double = record.getOrElse(ropKey, 1.0).toString.toDouble
    val wob: Double = record.getOrElse(wobKey, 1.0).toString.toDouble
    val dia: Double = record.getOrElse("diameter", 1.0).toString.toDouble
    val diaSquare = dia * dia

    //Mechanical Specific Energy
    // [(480)(Torque)(RPM)]/[(Dia^2)*(ROP)]    +    [4*(WOB)]/[pi*(Dia^2)]  = MSE
    // Centripital Energy    +    Axial Energy    =      MSE (ksi)

    val mse: String = String.valueOf((480 * tor * rpm) / (diaSquare * rop) + (4 * wob) / (diaSquare * scala.math.Pi))

    val mseKey=mnemonicName+"@"+record.getOrElse("LogName", "")
    record + ("CALC_MSE"->mseKey) + (mseKey -> mse)
  }

  def isValidRecord(record: Map[String, Object]): Boolean = {
    if (record.size == 0)
      return false

    return true
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

 /* def toMapOld(record: String): Map[String, String]= {
    var map = Map[String, String]()
    var result = JSON.parseRaw(record).getOrElse(null)
    if (result == null) {
      WARN(s"Record could not be parsed as a JSON object: ${record}")
      return map
    }
    var tmp = result.asInstanceOf[JSONObject].obj.asInstanceOf[Map[String, String]]
    var tmpIter = tmp.keySet.iterator
    var hashMap = new HashMap[String, String]

    //val secondPart = tmpKey//.split("@")(1)
    var logName = tmp.getOrElse("LogName","")
    var nameWell = tmp.getOrElse("nameWell","")
    var diameter = tmp.getOrElse("diameter","")

    var diameterDouble :Double = 0.0
    if(!diameter.isEmpty){
      try{
        diameterDouble = diameter.toDouble
      }catch{
        case  exp : Exception => exp.printStackTrace()
      }

    }
    hashMap.put("nameWell", nameWell)
    hashMap.put("nameLog", logName)
    hashMap.put("diameter", diameter)
    hashMap.put("tempus.tsds", "tempus.tsds")


    if(tmp.getOrElse(torKey, null) != null)
    hashMap.put(torKey, tmp.getOrElse(torKey, null))

    if(tmp.getOrElse(rpmKey, null) != null)
    hashMap.put(rpmKey, tmp.getOrElse(rpmKey, null))

    if(tmp.getOrElse(wobKey, null) != null)
    hashMap.put(wobKey, tmp.getOrElse(wobKey, null))

    if(tmp.getOrElse(ropKey, null) != null)
    hashMap.put(ropKey, tmp.getOrElse(ropKey, null))


    if (hashMap.size<8) { //We need at least 7 params: tempus.tsds,TOR,RPM,WOB,ROP,dia,nameWell
      DEBUG(s"Record does not have at least one of tempus.tsds, TOR, RPM, WOB, ROP, diameter, nameLog, or nameWell: ${record}")
      return map
    }
    var keyIter = hashMap.keysIterator
    while (keyIter.hasNext) {
      val key = keyIter.next()
      map = map + (key -> hashMap.getOrElse(key, null))
    }
    map
  }*/


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


  def main(args: Array[String]) : Unit = {

    var kafkaUrl = ""
    var topicName = ""
    var logLevel = ""
    var tokenName = ""
    var mqttUrl = ""

    var mqttTopic = ""


    try{
      val prop = new Properties()
      prop.load(new FileInputStream("kudu_witsml.properties"))

      kafkaUrl = prop.getProperty(TempusKuduConstants.KAFKA_URL_PROP)
      logLevel = prop.getProperty(TempusKuduConstants.LOG_LEVEL)
      topicName = prop.getProperty(TempusKuduConstants.TOPIC_MSE_PROP)
      tokenName = prop.getProperty(TempusKuduConstants.TEMPUS_MQTT_TOKEN)

      mqttUrl = prop.getProperty(TempusKuduConstants.TEMPUS_MQTT_URL)
      mqttTopic = prop.getProperty(TempusKuduConstants.TEMPUS_MQTT_TOPIC)

      torKey = prop.getProperty(TempusKuduConstants.MSE_TOR_KEY)
      wobKey = prop.getProperty(TempusKuduConstants.MSE_WOB_KEY)
      rpmKey = prop.getProperty(TempusKuduConstants.MSE_RPM_KEY)
      ropKey = prop.getProperty(TempusKuduConstants.MSE_ROP_KEY)
      mnemonicName = prop.getProperty(TempusKuduConstants.MSE_MNEMONIC)


      log.info(" kafkaUrl  --- >> "+kafkaUrl)
      log.info(" topicName  --- >> "+topicName)
      log.info(" mqttUrl --- >> "+mqttUrl)
      log.info(" tokenName --- >> "+tokenName)
      log.info(" mqttTopic --- >> "+mqttTopic)

      log.info(" mnemonicName --- >> "+mnemonicName)


      if(kafkaUrl == null  || topicName == null || mqttUrl == null|| tokenName == null || mqttTopic == null){
        log.info("  <<<--- kudu_witsml.properties file should be presented at classpath location with following properties " +
          "tempus.mqtt.url=tcp://<TempusServerIP>:1883\ntempus.mqtt.token=gatewaytoken\ntopic.witsml.mse=well-mse-data\nkafka.url=kafka:9092" +
          "\ntempus.mqtt.topic=v1/gateway/depth/telemetry >> ")
      }
      else{
        ComputeMSE.streamComputedMSE(kafkaUrl, Array(topicName),  mqttUrl, tokenName, mqttTopic, logLevel)
      }


    }catch{
      case  exp : Exception => exp.printStackTrace()
    }
 }
}
