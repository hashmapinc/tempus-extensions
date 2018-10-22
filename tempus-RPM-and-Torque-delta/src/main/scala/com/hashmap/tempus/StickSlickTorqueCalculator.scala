package com.hashmap.tempus

import java.lang

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.hashmap.tempus.annotations.SparkRequest
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Minutes, StreamingContext}
import org.eclipse.paho.client.mqttv3.MqttAsyncClient

@SparkRequest(main = "com.hashmap.tempus.StickSlickTorqueCalculator", jar = "uber-stickslickrpmandtorque-0.0.1-SNAPSHOT.jar",
  name="Stick slick Torque calculator", descriptor="StickSlickTorqueCalculatorActionDescriptor.json",
  args = Array("mqttUrl","kafkaUrl","kafkaTopic",
    "window", "gatewayAccessToken"))
class StickSlickTorqueCalculator

object StickSlickTorqueCalculator {

  private val log = Logger.getLogger(StickSlickTorqueCalculator.getClass)

  def main(args: Array[String]): Unit = {
    log.setLevel(Level.INFO)
    assert(args.length >= 5, ERROR("Missing required parameters for the job"))

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> args(1),
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> args(2),
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    val topics = Array(args(2))
    val winSize = Minutes(Integer.parseInt(args(3)))
    val mqttUrl = args(0)
    val gatewayAccessToken = args(4)

    val sparkConf = new SparkConf(true).setAppName("StickSlickTorqueCalculator")
    val ssc = new StreamingContext(sparkConf, winSize)
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    import spark.implicits._
    ssc.sparkContext.setLogLevel("INFO")

    val stream = KafkaUtils
      .createDirectStream[String, String](ssc, PreferConsistent, Subscribe[String, String](topics, kafkaParams))

    assert(stream != null, ERROR("Kafka stream is not available. Check your Kafka setup."))

    val result = stream.map(_.value()).map(deserialize).filter(_ != null)

    var maxTorque: Double = Double.MinValue
    var minTorque: Double = Double.MaxValue
    var deviceId: String = ""

    result.foreachRDD(rdd => {
      if (!rdd.isEmpty()) {
        deviceId = rdd.first().id
        maxTorque = math.max(maxTorque, rdd.max()(new Ordering[Data]() {
          override def compare(d1: Data, d2: Data): Int =
            Ordering[Double].compare(d1.currentTorque, d2.currentTorque)
        }).currentTorque)

        minTorque = math.min(minTorque, rdd.min()(new Ordering[Data]() {
          override def compare(d1: Data, d2: Data): Int =
            Ordering[Double].compare(d1.currentTorque, d2.currentTorque)
        }).currentTorque)
        publishData(mqttUrl, gatewayAccessToken, Values(maxTorque, minTorque, deviceId))
      }
    })

    ssc.start()
    ssc.awaitTermination()

  }

  private def publishData(mqttUrl: String, gatewayAccessToken: String, values: Values) = {
    val theClient: MqttAsyncClient = ThingsboardPublisher.connectToThingsBoard(mqttUrl, gatewayAccessToken)
    ThingsboardPublisher.publishTelemetryToThingsBoard(theClient, toDataJson(values.maxTorque, values.minTorque, values.maxTorque - values.minTorque, values.deviceId))
    ThingsboardPublisher.disconnect(theClient)
  }

  private def toDataJson(maxRpm: Double, minRpm: Double, delta: Double, deviceId: String): String = {
    val mapper = new ObjectMapper()
    val json = mapper.createObjectNode()
    val ja = json.putArray("Tank " + deviceId)
    var obj=ja.addObject()
    obj.put("ts", System.currentTimeMillis())
    obj=obj.putObject("values")
    obj.put("TRQP2PMAX", maxRpm)
    obj.put("TRQP2PMIN", minRpm)
    obj.put("TRQP2P", delta)
    mapper.writeValueAsString(json)
  }

  case class Values(maxTorque: Double, minTorque: Double, deviceId: String)

  case class Data(id: String, ts: String, currentTorque: Double)

  def deserialize(record: String): Data = {
    val objectMapper = new ObjectMapper()
    objectMapper.registerModule(DefaultScalaModule)
    objectMapper.readValue(record, classOf[Data])
  }

  def ERROR(s: String): Unit={
    log.error(s)
  }

}
