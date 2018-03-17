package com.hashmapinc.tempus

import java.io.FileInputStream
import java.util.Properties

import org.apache.log4j.Logger
import org.apache.spark.streaming.kafka010._
import com.fasterxml.jackson.databind.ObjectMapper
import com.hashmapinc.kafka.KafkaService
import com.hashmapinc.kudu.KuduService
import com.hashmapinc.spark.SparkService
import com.hashmapinc.util.{TempusKuduConstants, TempusUtils}


object ComputeMSE {

  val log = Logger.getLogger(ComputeMSE.getClass)

  var torKey :String = "";
  var wobKey :String = "";
  var rpmKey :String = "";
  var ropKey :String = "";
  var mnemonicName :String = "";


  def computeMSE(kafkaUrl: String, topics: Array[String], kuduUrl: String, kuduUser:String,kuduPassword:String,mqttUrl: String, gatewayToken: String, mqttTopic:String, groupId:String, timeWindow :String, level: String="WARN"): Unit = {

  //  log.setLevel(TempusKuduConstants.logLevelMap(level))
  //  TempusPublisher.setLogLevel(TempusKuduConstants.logLevelMap(level))
    val connection =  KuduService.getImpalaConnection(kuduUrl, kuduUser, kuduPassword)
    val spark = SparkService.getSparkSession("ComputeMSE")
    spark.sparkContext.setLogLevel(level)

    var timeWindowInt = 10
    if(!TempusUtils.isEmpty(timeWindow))
      timeWindowInt = timeWindow.toInt

    val streamContext = SparkService.getStreamContext(spark,timeWindowInt)
    streamContext.sparkContext.setLogLevel(level)

    import spark.implicits._
    val stream = KafkaService.readKafka(kafkaUrl, topics, kuduUrl,kuduUser,kuduPassword, groupId, streamContext)

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
      .filter(isValidRecord(_))
      .map(computeMSE(_))
      .filter(isValidRecord(_))
      .foreachRDD(rdd =>{
        if (!rdd.isEmpty()) {
          rdd.foreachPartition { p =>
            p.foreach(record => TempusPublisher.publishMSE(mqttUrl, gatewayToken, mqttTopic, toTempusData(record)))
          }
        }
      })

    streamContext.start()
    streamContext.awaitTermination()
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

    if(!keyValue.isEmpty)
      mseValue = keyValue.toDouble

    obj.put(key, mseValue)
    TempusUtils.INFO(s"Publishing: ${mapper.writeValueAsString(json)}")
    mapper.writeValueAsString(json)
  }

  def computeMSE(record: Map[String, String]): Map[String, String]={
    var mseKey :String = ""
    try{
      val tor: Double = record.getOrElse(torKey, -9999).toString.toDouble
      val rpm: Double = record.getOrElse(rpmKey, -9999).toString.toDouble
      val rop: Double = record.getOrElse(ropKey, -9999).toString.toDouble
      val wob: Double = record.getOrElse(wobKey, -9999).toString.toDouble
      val defaultDia :String = "diameter"
      var logName: String = record.getOrElse("LogName", "")
      logName = logName.replaceAll(" ","")
      val diaLogName: String = defaultDia+logName
      var dia: Double = record.getOrElse(diaLogName, -9999).toString.toDouble

     if(dia == -9999)
        dia = record.getOrElse(defaultDia, -9999).toString.toDouble

      if(tor == -9999 ||  rpm == -9999  ||  rop == -9999 || wob == -9999 ||  dia == -9999){
        log.error(" Not a valid value to proceed for MSE computation tor => "+tor+" rpm => "+rpm+" rop => "+rop +" wob => "+wob+" diameter => "+dia)
        null
      }else{

        val diaSquare = dia * dia
        mseKey=mnemonicName+"@"+logName
        //Mechanical Specific Energy
        // [(480)(Torque)(RPM)]/[(Dia^2)*(ROP)]    +    [4*(WOB)]/[pi*(Dia^2)]  = MSE
        // Centripital Energy    +    Axial Energy    =      MSE (ksi)

        val centEnergy = (480 * tor * rpm) / (diaSquare * rop)
        val axialEnergy = (4 * wob) / (diaSquare * scala.math.Pi)

        val mse: String = String.valueOf(centEnergy + axialEnergy)
        record + ("CALC_MSE"->mseKey) + (mseKey -> mse)
      }
    }catch{
      case  exp : Exception => exp.printStackTrace()
       // record + ("CALC_MSE"->mseKey) + (mseKey -> "0")
      null
    }
  }
  def isValidRecord(record: Map[String, Object]): Boolean = {
    if (record == null || record.size == 0)
      return false

    return true
  }


  def main(args: Array[String]) : Unit = {

    var kafkaUrl = ""
    var topicName = ""
    var logLevel = ""
    var tokenName = ""
    var mqttUrl = ""
    var mqttTopic = ""
    var kuduConnectionUrl = ""
    var kuduConnectionUser = ""
    var kuduConnectionPassword = ""
    var groupId = ""
    var timeWindow = ""

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
      kuduConnectionUrl = prop.getProperty(TempusKuduConstants.KUDU_IMPALA_CONNECTION_URL_PROP)
      kuduConnectionUser = prop.getProperty(TempusKuduConstants.KUDU_CONNECTION_USER_PROP)
      kuduConnectionPassword = prop.getProperty(TempusKuduConstants.KUDU_CONNECTION_PASSWORD_PROP)
      groupId =  prop.getProperty(TempusKuduConstants.MSE_KAFKA_GROUP)
      timeWindow =  prop.getProperty(TempusKuduConstants.MSE_TIME_WINDOW)


      log.info(" kafkaUrl  --- >> "+kafkaUrl)
      log.info(" topicName  --- >> "+topicName)
      log.info(" mqttUrl --- >> "+mqttUrl)
      log.info(" tokenName --- >> "+tokenName)
      log.info(" mqttTopic --- >> "+mqttTopic)
      log.info(" mnemonicName --- >> "+mnemonicName)
      log.info(" kuduConnectionUrl --- >> "+kuduConnectionUrl)
      log.info(" kuduConnectionUser --- >> "+kuduConnectionUser)
      log.info(" kuduConnectionPassword --- >> "+kuduConnectionPassword)
      log.info(" groupId --- >> "+groupId)
      log.info(" timeWindow --- >> "+timeWindow)

      if(TempusUtils.isEmpty(kafkaUrl) || TempusUtils.isEmpty(topicName)  || TempusUtils.isEmpty(mqttUrl)
        || TempusUtils.isEmpty(tokenName)  || TempusUtils.isEmpty(mqttTopic)){
        log.info("  <<<--- kudu_witsml.properties file should be presented at classpath location with following properties " +
          "tempus.mqtt.url=tcp://<TempusServerIP>:1883\ntempus.mqtt.token=gatewaytoken\ntopic.witsml.mse=well-mse-data\nkafka.url=kafka:9092" +
          "\ntempus.mqtt.topic=v1/gateway/depth/telemetry >> ")
      }
      else{
        ComputeMSE.computeMSE(kafkaUrl, Array(topicName), kuduConnectionUrl,kuduConnectionUser,kuduConnectionPassword, mqttUrl, tokenName, mqttTopic, groupId, timeWindow,logLevel)
      }


    }catch{
      case  exp : Exception => exp.printStackTrace()
    }
 }
}
