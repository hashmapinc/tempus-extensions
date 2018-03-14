package com.hashmapinc.tempus.util

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, HasOffsetRanges, KafkaUtils}
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent

/**
  * @author Mitesh Rathore
  */
object KafkaService {





  def readKafka(kafkaUrl: String, topics: Array[String], impalaKuduUrl:String,kuduUser:String,kuduPassword:String, groupId :String, streamContext : StreamingContext) : InputDStream[ConsumerRecord[String, String]]  = {
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> kafkaUrl , //kafka,
      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer", //classOf[StringDeserializer],
      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer", //classOf[StringDeserializer],
      "group.id" -> groupId,
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )


    //val ssc = new StreamingContext(sparkContext, Seconds(10))
    val con =  TempusUtils.getImpalaConnection(impalaKuduUrl, kuduUser, kuduPassword)
    val fromOffsets= TempusUtils.getLastCommittedOffsets(con,topics(0),groupId)
    val stream = KafkaUtils.createDirectStream[String, String](streamContext , PreferConsistent, ConsumerStrategies.Subscribe[String, String](topics, kafkaParams,fromOffsets))
    stream




  }

}
