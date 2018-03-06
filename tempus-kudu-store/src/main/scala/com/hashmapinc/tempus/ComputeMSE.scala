package com.hashmapinc.tempus

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

object ComputeMSE {
  val logLevelMap = Map("INFO"->Level.INFO, "WARN"->Level.WARN, "DEBUG"->Level.DEBUG)
  val log = Logger.getLogger(ToKudu.getClass)
  val MQTT_TOPIC: String = "v1/gateway/depth/telemetry"

  def streamComputedMSE(kafka: String, topics: Array[String], groupid: String, mqttUrl: String, gatewayToken: String, level: String="WARN"): Unit = {
    log.setLevel(logLevelMap(level))
    TempusPublisher.setLogLevel(logLevelMap(level))
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> kafka,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> groupid,
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    val sparkConf = new SparkConf().setAppName("ComputeMSE")
    val ssc = new StreamingContext(sparkConf, Minutes(1))
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

    values.foreachRDD(rdd =>{
      if (!rdd.isEmpty()) {
        rdd.foreachPartition { p =>
          val con = TempusPublisher.connect(mqttUrl, gatewayToken)

          p.foreach(record => TempusPublisher.publishMSE(con, MQTT_TOPIC, toTempusData(record)))

          con.disconnect()
        }
      }
    })

    ssc.start()
    ssc.awaitTermination()
  }

  def toTempusData(record: Map[String, String]): String = {
    val mapper = new ObjectMapper()
    val json = mapper.createObjectNode()
    var ja = json.putArray(record.getOrElse("nameWell", "nameWell"))
    var obj=ja.addObject()
    obj.put("ds", record.getOrElse("tempus.tsds", "0.00"))
    obj=obj.putObject("values")
    val key=record.getOrElse("MSE", "MSE")
    obj.put(key, record.getOrElse(key, "0.00"))
    INFO(s"Publishing: ${mapper.writeValueAsString(json)}")
    mapper.writeValueAsString(json)
  }

  def computeMSE(record: Map[String, String]): Map[String, String]={
    val tor: Double = record.getOrElse("TOR", 1.0).toString.toDouble
    val rpm: Double = record.getOrElse("RPM", 1.0).toString.toDouble
    val rop: Double = record.getOrElse("ROP", 1.0).toString.toDouble
    val wob: Double = record.getOrElse("WOB", 1.0).toString.toDouble
    val dia: Double = record.getOrElse("diameter", 1.0).toString.toDouble
    val diaSquare = dia * dia
    val mse: String = String.valueOf((480 * tor * rpm) / (diaSquare * rop) + (4 * wob) / (diaSquare * scala.math.Pi))
    val mseKey="MSE@"+record.getOrElse("nameLog", "")
    record + ("MSE"->mseKey) + (mseKey -> mse)
  }

  def isValidRecord(record: Map[String, Object]): Boolean = {
    if (record.size == 0)
      return false

    return true
  }

  def toMap(record: String): Map[String, String]= {
    var map = Map[String, String]()
    var result = JSON.parseRaw(record).getOrElse(null)
    if (result == null) {
      WARN(s"Record could not be parsed as a JSON object: ${record}")
      return map
    }
    var tmp = result.asInstanceOf[JSONObject].obj.asInstanceOf[Map[String, String]]
    var tmpIter = tmp.keySet.iterator
    var hashMap = new HashMap[String, String]
    while (tmpIter.hasNext) {
      val tmpKey = tmpIter.next()
      val firstPart = tmpKey.split("@")(0)
      firstPart match {
        case "tempus.tsds" | "TOR" | "RPM" | "WOB" =>
          hashMap.put(firstPart, tmp.getOrElse(tmpKey, null))
        case "ROP" =>
          val secondPart = tmpKey.split("@")(1)
          val rop:String = tmp.getOrElse(tmpKey, null)
          val dia = getDiameter(tmp.getOrElse("ss", null), "diameter."+secondPart)
          if (rop.toDouble > 0 && dia.toDouble > 0) {
            hashMap.put(firstPart, rop)
            hashMap.put("diameter", dia)
            hashMap.put("nameLog", secondPart)
          }
        case _ =>
      }
    }
    getWellName(tmp, "nameWell", hashMap)
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
  }

  def getWellName(source: Map[String, String], name: String, target: HashMap[String, String]): Unit = {
    val rec=source.getOrElse("cs", null)
    if (rec==null) return
    var key=name+"="
    var dataStartIndex=rec.indexOf(key)+key.length
    var dataEndIndex=rec.indexOf(", ", dataStartIndex)
    if (dataEndIndex<0)
      dataEndIndex=rec.indexOf("}", dataStartIndex)
    DEBUG(s"${rec.substring(dataStartIndex, dataEndIndex)}")
    target.put(name, rec.substring(dataStartIndex, dataEndIndex))
  }

  def getDiameter(rec: String, name: String): String = {
    if (rec==null || rec.isEmpty) return "0"
    var key=name+"="
    var dataStartIndex=rec.indexOf(key)+key.length
    var dataEndIndex=rec.indexOf(", ", dataStartIndex)
    if (dataEndIndex<0)
      dataEndIndex=rec.indexOf("}", dataStartIndex)
    DEBUG(s"${rec.substring(dataStartIndex, dataEndIndex)}")
    return rec.substring(dataStartIndex, dataEndIndex)
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


  def main(args: Array[String]) : Unit = {
    ComputeMSE.streamComputedMSE(args(0), args(1).split(","), args(2), args(3), args(4), args(5))
  }
}
