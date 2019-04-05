package com.hashmapinc.tempus

import java.lang

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent

import scala.util.{Failure, Success, Try}

object SparkKafkaApplication extends StreamingContextHelper{

  def run(options: OptionsMap, j: OptionsMap => Try[Job]): Unit = {

    val errorMsgTemplate = "Required Parameter %s missing"
    val kafkaBroker = options.getOrElse("kafkaUrl", throw new Exception(String.format(errorMsgTemplate, "Kafka Broker")))
    val topic = options.getOrElse("kafkaTopic", throw new Exception(String.format(errorMsgTemplate, "Kafka topic")))
    val kafkaParams: Map[String, Object] = kafkaConnectionParams(kafkaBroker, topic)

    j(options) match {
      case Success(job) =>
        withContext(options){ ssc =>
          val stream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils
            .createDirectStream[String, String](ssc, PreferConsistent,
            Subscribe[String, String](Array(topic.asInstanceOf[String]), kafkaParams))
          job.withStreamingContext(ssc)(stream.map(_.value()))
        }
      case Failure(e) => throw e
    }

  }

  private def kafkaConnectionParams(kafkaBroker: String, topic: String): Map[String, Object] = {
    Map[String, Object](
      "bootstrap.servers" -> kafkaBroker, //"kafka:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> topic, //"DEFAULT_GROUP_ID",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: lang.Boolean))
  }
}
