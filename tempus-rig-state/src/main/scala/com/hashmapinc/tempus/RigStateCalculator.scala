package com.hashmapinc.tempus

import java.lang
import java.util.Optional

import com.google.gson.{Gson, GsonBuilder}
import com.hashmap.tempus.annotations.SparkRequest
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Milliseconds, StreamingContext}

import scala.math.abs

case class RigStateData(id: String, ts: Long, bitDepth: Double, holeDepth: Double, totalPumpOutput: Double, rotaryRpm: Double, inSlipStatus: Int, standpipePressure: Double)
case class PublishRigStateData(RIGSTATE: String)

@SparkRequest(main = "com.hashmapinc.tempus.RigStateCalculator", jar = "uber-tempus-rig-state-0.0.1-SNAPSHOT.jar",
  name = "Rig State", descriptor = "RigStateDescriptor.json",
  args = Array("mqttUrl", "kafkaUrl", "kafkaTopic", "gatewayAccessToken"))
class RigStateCalculator

object RigStateCalculator {

  private val log = Logger.getLogger(RigStateCalculator.getClass)

  def main(args: Array[String]): Unit = {

    assert(args.length >= 4, ERROR("Usage: mqttUrl, kafkaUrl, kafkaTopic, gatewayAccessToken.\nTry mqttyUrl as tcp://tb:1883, kafkaUrl as kafka:9092, topic as rig-state-data"))

    val mqttUrl = args(0)
    val gatewayAccessToken = args(3)

    val kafkaTopic = args(2)
    val kafkaUrl = args(1)

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> kafkaUrl, //"kafka:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> kafkaTopic, //"DEFAULT_GROUP_ID",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val checkPointDir = System.getenv().getOrDefault("CHECKPOINT_DIR", ".")

    val sparkConf = new SparkConf().setAppName("RigStateCalculator")
    val windowDuration = Milliseconds(5000)
    val ssc = new StreamingContext(sparkConf, windowDuration)
    ssc.sparkContext.setLogLevel("WARN")
    ssc.checkpoint(checkPointDir)

    val stream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils
      .createDirectStream[String, String](ssc, PreferConsistent, Subscribe[String, String](Array(kafkaTopic), kafkaParams))

    assert((stream != null), ERROR("Kafka stream is not available. Check your Kafka setup."))

    stream.map(r => r.value()).filter(!_.trim.isEmpty).map(parseAndPairByKey).updateStateByKey[RigStateData](processAndUpdateState _).print()

    ssc.start()
    ssc.awaitTermination()

    def processAndUpdateState(newRSD: Seq[RigStateData], prevRSD: Option[RigStateData]): Option[RigStateData] = {
      if(newRSD.nonEmpty) newRSD.foldLeft(prevRSD)(processRecord) else prevRSD
    }

    def processRecord(prevRSD: Option[RigStateData], currRSD: RigStateData): Option[RigStateData] = {
      if (prevRSD.isEmpty) {
        Some(currRSD)
      } else {

        val rigState = calculateRigState(prevRSD.get, currRSD)
        val data = PublishRigStateData(rigState)

        publishToTempusCloud(currRSD, data)

        prevRSD
      }
    }

    def publishToTempusCloud(currRSD: RigStateData, data: PublishRigStateData) = {
      val json = new GsonBuilder().create.toJson(data)
      val empty: Optional[lang.Double] = Optional.ofNullable(null)

      new MqttConnector(mqttUrl, gatewayAccessToken).publish(json, Optional.of(currRSD.ts), empty, currRSD.id)

      INFO(s"Published data to mqtt server: $mqttUrl.value with payload $data ")
    }
  }


  def calculateRigState(prevRSD: RigStateData, currRSD: RigStateData) = {
    if (currRSD.bitDepth == -999.25 && currRSD.holeDepth == -999.25) "Depth Data Issue"
    else if (currRSD.bitDepth <= 0 && currRSD.bitDepth != -999.25 && currRSD.holeDepth > 0) "Out of Hole"
    else {
      val pipeDirection = calculatePipeDirection(prevRSD, currRSD)
      val pumpStatus = calculatePumpStatus(currRSD)
      val topDriveStatus = calculateTopDriveStatus(currRSD)
      val bitOnBottom = calculateBitOnBottom(currRSD)

      if (pipeDirection == 0 && pumpStatus == -1 && topDriveStatus == -1 && currRSD.inSlipStatus == 0 && bitOnBottom == 1) "Stationary"
      else if (pipeDirection == 0 && pumpStatus == -1 && topDriveStatus == 1 && currRSD.inSlipStatus == 0 && bitOnBottom == 1) "Rotating"
      else if (pipeDirection == 0 && pumpStatus == 1 && topDriveStatus == 1 && currRSD.inSlipStatus == 0 && bitOnBottom == 1) "Rotating Pumping"
      else if (pipeDirection == 0 && pumpStatus == 1 && topDriveStatus == -1 && bitOnBottom == 1) "Pumping"
      else if (pipeDirection == -1 && pumpStatus == 1 && topDriveStatus == 1 && currRSD.inSlipStatus == 0 && bitOnBottom == 0) "Rotary Drilling"
      else if (pipeDirection == -1 && pumpStatus == 1 && topDriveStatus == -1 && currRSD.inSlipStatus == 0 && bitOnBottom == 0) "Slide Drilling"
      else if (pipeDirection == 1 && pumpStatus == 1 && topDriveStatus == -1 && currRSD.inSlipStatus == 0 && bitOnBottom == 1) "Tripping Out Pumping"
      else if (pipeDirection == 1 && pumpStatus == -1 && topDriveStatus == -1 && currRSD.inSlipStatus == 0 && bitOnBottom == 1) "Tripping Out"
      else if (pipeDirection == 1 && pumpStatus == -1 && topDriveStatus == 1 && currRSD.inSlipStatus == 0 && bitOnBottom == 1) "Tripping Out Rotating"
      else if (pipeDirection == -1 && pumpStatus == 1 && topDriveStatus == -1 && currRSD.inSlipStatus == 0 && bitOnBottom == 1) "Tripping In Pumping"
      else if (pipeDirection == -1 && pumpStatus == -1 && topDriveStatus == -1 && currRSD.inSlipStatus == 0 && bitOnBottom == 1) "Tripping In"
      else if (pipeDirection == -1 && pumpStatus == -1 && topDriveStatus == 1 && currRSD.inSlipStatus == 0 && bitOnBottom == 1) "Tripping In Rotating"
      else if (currRSD.inSlipStatus == 1 && bitOnBottom == 1) "In Slips"
      else if (pipeDirection == 1 && pumpStatus == 1 && topDriveStatus == 1 && currRSD.inSlipStatus == 0 && bitOnBottom == 1) "Back Ream"
      else if (pipeDirection == -1 && pumpStatus == 1 && topDriveStatus == 1 && currRSD.inSlipStatus == 0 && bitOnBottom == 1) "Ream In"
      else "Unknown"
    }
  }

  private def calculatePipeDirection(previousRigStateData: RigStateData, currentRigStateData: RigStateData) = {
    if (abs(currentRigStateData.bitDepth - previousRigStateData.bitDepth) <= 0.1) 0
    else if (currentRigStateData.bitDepth - previousRigStateData.bitDepth > 0) -1
    else if (currentRigStateData.bitDepth - previousRigStateData.bitDepth < 0) 1
    else Integer.MAX_VALUE
  }

  private def calculatePumpStatus(currentRigStateData: RigStateData) = {
    if (currentRigStateData.totalPumpOutput <= 0.00630902) -1
    else if (currentRigStateData.totalPumpOutput > 0.00630902 && currentRigStateData.standpipePressure >= 3447380) 1
    else Integer.MAX_VALUE
  }

  private def calculateTopDriveStatus(currentRigStateData: RigStateData) = {
    if (currentRigStateData.rotaryRpm <= 1.05) -1
    else if (currentRigStateData.rotaryRpm > 3.15) 1
    else if (currentRigStateData.rotaryRpm > 1.05 && currentRigStateData.rotaryRpm  <= 3.15) 0
    else Integer.MAX_VALUE
  }

  private def calculateBitOnBottom(currentRigStateData: RigStateData) = {
    if (currentRigStateData.holeDepth - currentRigStateData.bitDepth < (-0.3048)) -1
    else if (currentRigStateData.holeDepth - currentRigStateData.bitDepth < 0.33) 0
    else 1
  }

  def parseAndPairByKey(jsonStr: String): (String, RigStateData) = {
    val data: RigStateData = new Gson().fromJson(jsonStr.trim, classOf[RigStateData])
    (data.id, data)
  }

  private def INFO(s: String): Unit = {
    if (log.isInfoEnabled) {
      log.info(s)
    }
  }

  private def ERROR(s: String): Unit = {
    log.error(s)
  }
}