package com.hashmapinc.kafka

import com.hashmapinc.kudu.KuduService
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils}

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

    val con =  KuduService.getImpalaConnection(impalaKuduUrl, kuduUser, kuduPassword)
    val fromOffsets= KuduService.getOffsets(con,topics(0),groupId)
    val stream = KafkaUtils.createDirectStream[String, String](streamContext , PreferConsistent, ConsumerStrategies.Subscribe[String, String](topics, kafkaParams,fromOffsets))
    stream
  }

}
