package com.hashmap.tempus

import com.hashmap.tempus.annotations.SparkRequest
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Minutes, StreamingContext}
import org.codehaus.jackson.map.ObjectMapper
import org.eclipse.paho.client.mqttv3.MqttAsyncClient

import scala.util.parsing.json.{JSON, JSONObject}

@SparkRequest(main = "com.hashmap.tempus.StickSlickRPMCalculator", jar = "uber-stickslickrpm-0.0.1-SNAPSHOT.jar",
  name="Stick slick RPM calculator", descriptor="StickSlickRPMCalculatorActionDescriptor.json",
  args = Array("mqttUrl","kafkaUrl","kafkaTopic",
    "window", "gatewayAccessToken"))
class StickSlickRPMCalculator

  object StickSlickRPMCalculator {
    val TS="ts"
    val MAX_RPM="maxRpm"
    val MIN_RPM="minRpm"

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
      val winSize = Minutes(Integer.parseInt(args(4)))
      val mqttUrl = args(0)
      val gatewayAccessToken = args(5)

      val sparkConf = new SparkConf().setAppName("StickSlickRPMCalculator")
      val ssc = new StreamingContext(sparkConf, winSize)
      val spark = SparkSession.builder().config(sparkConf).getOrCreate()
      ssc.sparkContext.setLogLevel("INFO")

      val stream = KafkaUtils
        .createDirectStream[String, String](ssc, PreferConsistent, Subscribe[String, String](topics, kafkaParams))

      assert(stream != null, ERROR("Kafka stream is not available. Check your Kafka setup."))

      val result = stream.map(_.value()).map(deserialize).filter(_ != null).map(d => (d.ts, d.maxRpm - d.minRpm))

      var theClient:MqttAsyncClient = null
      result.foreachRDD(rdd =>{
        if (!rdd.isEmpty()) {

          rdd.foreachPartition { p =>
            theClient = ThingsboardPublisher.connectToThingboard(mqttUrl, gatewayAccessToken)

            p.foreach(r => ThingsboardPublisher.publishTelemetryToThingsboard(theClient, r))

            ThingsboardPublisher.disconnect(theClient)
          }

        }
      })

    }

    case class Data(ts: Long, maxRpm: Double, minRpm: Double)

    def deserialize(record: String): Data = {
      val objectMapper = new ObjectMapper()
      objectMapper.readValue(record, classOf[Data])
    }

    def ERROR(s: String): Unit={
      log.error(s)
    }

  }
