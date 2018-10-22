package com.hashmap.tempus

import java.lang
import java.util.Optional

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.google.gson.GsonBuilder
import com.hashmap.tempus.annotations.SparkRequest
import com.hashmapinc.tempus.MqttConnector
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Minutes, StreamingContext}

@SparkRequest(main = "com.hashmap.tempus.StickSlickRPMCalculator", jar = "uber-stickslickrpmandtorque-0.0.1-SNAPSHOT.jar",
  name="Stick slick RPM calculator", descriptor="StickSlickRPMCalculatorActionDescriptor.json",
  args = Array("mqttUrl","kafkaUrl","kafkaTopic",
    "window", "gatewayAccessToken"))
class StickSlickRPMCalculator

  object StickSlickRPMCalculator {

    private val log = Logger.getLogger(StickSlickRPMCalculator.getClass)

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

      val sparkConf = new SparkConf(true).setAppName("StickSlickRPMCalculator")
      val ssc = new StreamingContext(sparkConf, winSize)
      val spark = SparkSession.builder().config(sparkConf).getOrCreate()
      import spark.implicits._
      ssc.sparkContext.setLogLevel("INFO")

      val stream = KafkaUtils
        .createDirectStream[String, String](ssc, PreferConsistent, Subscribe[String, String](topics, kafkaParams))

      assert(stream != null, ERROR("Kafka stream is not available. Check your Kafka setup."))

      val result = stream.map(_.value()).map(deserialize).filter(_ != null)

      var maxRpm: Double = Double.MinValue
      var minRpm: Double = Double.MaxValue

      result.foreachRDD(rdd => {
        if (!rdd.isEmpty()) {
          maxRpm = math.max(maxRpm, rdd.max()(new Ordering[Data]() {
            override def compare(d1: Data, d2: Data): Int =
              Ordering[Double].compare(d1.currentRpm, d2.currentRpm)
          }).currentRpm)

          minRpm = math.min(minRpm, rdd.min()(new Ordering[Data]() {
            override def compare(d1: Data, d2: Data): Int =
              Ordering[Double].compare(d1.currentRpm, d2.currentRpm)
          }).currentRpm)
          publishData(mqttUrl, gatewayAccessToken, Value(maxRpm, minRpm, maxRpm - minRpm), rdd.first().ts, rdd.first().id)
        }
      })

      ssc.start()
      ssc.awaitTermination()

    }

    def publishData(mqttUrl: String, gatewayAccessToken: String, value: Value, ts: String, deviceId: String): Unit = {
      val json = new GsonBuilder().create.toJson(value)
      val empty: Optional[lang.Double] = Optional.ofNullable(null)

      new MqttConnector(mqttUrl, gatewayAccessToken).publish(json, Optional.of(ts.toLong), empty, deviceId)

      INFO(s"Published data to mqtt server: $mqttUrl with payload $value ")
    }

    case class Value(RPMP2P: Double, RPMP2PMAX: Double, RPMP2PMIN: Double)

    case class Data(id: String, ts: String, currentRpm: Double)

    def deserialize(record: String): Data = {
      val objectMapper = new ObjectMapper()
      objectMapper.registerModule(DefaultScalaModule)
      objectMapper.readValue(record, classOf[Data])
    }

    def ERROR(s: String): Unit={
      log.error(s)
    }

    private def INFO(s: String): Unit = {
      if (log.isInfoEnabled) {
        log.info(s)
      }
    }

  }
