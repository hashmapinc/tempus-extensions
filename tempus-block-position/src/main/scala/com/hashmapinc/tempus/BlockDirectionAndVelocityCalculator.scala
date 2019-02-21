package com.hashmapinc.tempus

import java.lang
import java.util.Optional

import com.google.gson.{Gson, GsonBuilder}
import com.hashmap.tempus.annotations.SparkRequest
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Milliseconds, StreamingContext}

case class BlockPositionData(id: String, ts: Long, blockPosition: Double)
case class PublishPositionData(BDIR: Int, BDIRTEXT: String, BVEL: Double)

@SparkRequest(main = "com.hashmapinc.tempus.BlockDirectionAndVelocityCalculator", jar = "uber-tempus-block-position-0.0.1-SNAPSHOT.jar",
  name = "Block direction and velocity calculator", descriptor = "BlockDirectionAndVelocityActionDescriptor.json",
  args = Array("mqttUrl", "kafkaUrl", "kafkaTopic", "minComputationWindow", "maxComputationWindow", "gatewayAccessToken"))
class BlockDirectionAndVelocityCalculator

object BlockDirectionAndVelocityCalculator {

  private val log = Logger.getLogger(BlockDirectionAndVelocityCalculator.getClass)

  def main(args: Array[String]): Unit = {

    assert(args.length >= 6, ERROR("Usage: mqttUrl, kafkaUrl, kafkaTopic, minComputationWindow, maxComputationWindow, gatewayAccessToken.\nTry mqttyUrl as tcp://tb:1883, kafkaUrl as kafka:9092, topic as block-position-data, minComputationWindow as 1 and maxComputationWindow as 60"))

    val mqttUrl = args(0)
    val minComputationWindow = args(3).trim().toInt * 1000L
    val maxComputationWindow = args(4).trim().toInt * 1000L
    val gatewayAccessToken = args(5)

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

    val sparkConf = new SparkConf().setAppName("BlockDirectionCalculator")
    val windowDuration = Milliseconds(800)
    val ssc = new StreamingContext(sparkConf, windowDuration)
    ssc.sparkContext.setLogLevel("WARN")
    ssc.checkpoint(checkPointDir)

    SparkSession.builder().config(sparkConf).getOrCreate()

    val stream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils
      .createDirectStream[String, String](ssc, PreferConsistent, Subscribe[String, String](Array(kafkaTopic), kafkaParams))

    assert((stream != null), ERROR("Kafka stream is not available. Check your Kafka setup."))

    stream.map(r => r.value()).filter(!_.trim.isEmpty).map(parseAndPairByKey).updateStateByKey[BlockPositionData](processAndUpdateState _).print()

    ssc.start()
    ssc.awaitTermination()

    def processAndUpdateState(newPositions: Seq[BlockPositionData], previousPosition: Option[BlockPositionData]): Option[BlockPositionData] = {
      if(newPositions.nonEmpty) newPositions.foldLeft(previousPosition)(processRecord) else previousPosition
    }

    def processRecord(previousPosData: Option[BlockPositionData], currentPosData: BlockPositionData): Option[BlockPositionData] = {

      if (previousPosData.isEmpty || currentPosData.ts - previousPosData.get.ts > maxComputationWindow) {
        Some(currentPosData)
      } else if (currentPosData.ts - previousPosData.get.ts >= minComputationWindow && currentPosData.ts - previousPosData.get.ts <= maxComputationWindow) {

        val (direction: Int, directionText: String) = calculateDirection(previousPosData.get, currentPosData)
        val velocity = calculateVelocity(previousPosData.get, currentPosData)
        val data = PublishPositionData(direction, directionText, velocity)

        publishToTempusCloud(currentPosData, data)

        previousPosData
      } else {
        previousPosData
      }
    }

    def publishToTempusCloud(currentPosData: BlockPositionData, data: PublishPositionData) = {
      val json = new GsonBuilder().create.toJson(data)
      val empty: Optional[lang.Double] = Optional.ofNullable(null)

      new MqttConnector(mqttUrl, gatewayAccessToken).publish(json, Optional.of(currentPosData.ts), empty, currentPosData.id)

      INFO(s"Published data to mqtt server: $mqttUrl with payload $data ")
    }
  }


  def calculateDirection(previousPosData: BlockPositionData, currentPosData: BlockPositionData) = {
    if (currentPosData.blockPosition - previousPosData.blockPosition > 0) (1, "up")
    else if (currentPosData.blockPosition - previousPosData.blockPosition == 0) (0, "stopped")
    else (-1, "down")
  }

  def calculateVelocity(previousPosData: BlockPositionData, currentPosData: BlockPositionData) = {
    Math.abs((currentPosData.blockPosition - previousPosData.blockPosition) / ((currentPosData.ts - previousPosData.ts) / 1000))
  }

  def parseAndPairByKey(jsonStr: String): (String, BlockPositionData) = {
    val data: BlockPositionData = new Gson().fromJson(jsonStr.trim, classOf[BlockPositionData])
    (data.id, data)
  }

  private def INFO(s: String): Unit = {
    if (log.isInfoEnabled()) {
      log.info(s)
    }
  }

  private def ERROR(s: String): Unit = {
    log.error(s)
  }
}