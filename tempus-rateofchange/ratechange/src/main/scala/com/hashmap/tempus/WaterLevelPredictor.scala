package com.hashmap.tempus

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.eclipse.paho.client.mqttv3.{IMqttActionListener, IMqttToken, MqttAsyncClient, MqttConnectOptions, MqttException, MqttMessage}
import java.nio.charset.StandardCharsets

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.log4j.Logger
import org.apache.log4j.Level

import scala.util.parsing.json.{JSON, JSONObject}

case class WaterTankLevelDataSet(tankId: String, ts: Long, waterTankLevel: Double)

object WaterLevelPredictor {
  val TANKID="tankId"
  val TS="ts"
  val WATERTANKLEVEL="waterTankLevel"
  val log = Logger.getLogger(WaterLevelPredictor.getClass)
  
  def main(args: Array[String]): Unit = {
    assert(args.length>=5, ERROR("Usage: mqttUrl kafkaUrl kafkaTopic highWaterMark windowSize optionalDebugLevel.\nTry mqttyUrl as tcp://tb:1883, kafkaUrl as kafka:9092, topic as water-tank-level-data, tankId as 123, highWaterMark as 70.0, windowSize as 1, and debuglevel as DEBUG=INFO"))

    if (args.length>5) {
      val level = args(5).split("=")(1).trim()
      if (level.equalsIgnoreCase("INFO"))
        log.setLevel(Level.INFO)
        ThingsboardPublisher.setLogLevel(Level.INFO)
    }
    val mqttUrl = args(0)
    val kafkaParams = Map[String, Object](
     "bootstrap.servers" -> args(1), //"kafka:9092",
     "key.deserializer" -> classOf[StringDeserializer],
     "value.deserializer" -> classOf[StringDeserializer],
     "group.id" -> args(2), //"DEFAULT_GROUP_ID",
     "auto.offset.reset" -> "latest",
     "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    val topics = Array(args(2)) 
    val tankId = args(3)
    val highWaterMark = args(3).trim().toDouble
    val winSize = Minutes(Integer.parseInt(args(4)))
    val batchSize = winSize //Seconds(20)
  
    val sparkConf = new SparkConf().setAppName("WaterLevelPredictor")
    val ssc = new StreamingContext(sparkConf, batchSize)
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    import spark.implicits._
    ssc.sparkContext.setLogLevel("WARN")

    
    val stream = KafkaUtils
                    .createDirectStream[String, String](ssc, PreferConsistent, Subscribe[String, String](topics, kafkaParams))
    
    assert((stream != null), ERROR("Kafka stream is not available. Check your Kafka setup."))
    
    //Stream could be empty but that is perfectly okay
    val values  = stream.map(_.value()).map(stringize(_)).filter(_.length>0)

    //At this point in time, records is expected to have 0 or more elements
    //in each batch of RDDs for the specified any number of tanks and so let us consolidate by tankId
    val result = values.map(r => (r.split(",")(0), r))
                        .reduceByKeyAndWindow((r1:String, r2:String)=>if (r1.length()>0 && r2.length()>0) r1+","+r2 else r1+r2, winSize, winSize)
                        .map(r=>(r._1, getRunningAverage(r._2)))
    
    //Now that the window data is processed, let us publish data to mqtt                    
    var theClient:MqttAsyncClient = null
    result.foreachRDD(rdd =>{
      if (!rdd.isEmpty()) {
        rdd.foreachPartition { p =>
          theClient = ThingsboardPublisher.connectToThingboard(mqttUrl)

          //Now publish data for each tankId
          p.foreach(r => ThingsboardPublisher.publishTelemetryToThingsboard(theClient, r, highWaterMark, getEta(r, highWaterMark)))
          
          ThingsboardPublisher.disconnect(theClient)
        }
      }
    })
  
    ssc.start()
    ssc.awaitTermination()
  }  
  
  def getRunningAverage(s: String): (Long, Double, Double) = {
    INFO(s"Records to process are: ${s}")
    var avgRate: Double = 0.0
    var first_time = true
    var ar: Array[String] = s.split(",")
    var ts = 0L
    var tslevel: Double = 0.0
    if (ar.length > 2) {
      ts = ar(1).trim().toLong
      tslevel = ar(2).trim().toDouble
    }
    for (i <- Range(0, ar.length/3-1)) {
      var interval = ar(i*3+1).trim().toLong-ar(i*3+4).trim().toLong
      if (interval != 0) {
        var increment = (ar(i*3+2).trim().toDouble-ar(i*3+5).trim().toDouble)*1000
        if (interval != 0) {
          if (first_time) {
            avgRate = increment / interval
            first_time=false
          }
          else {
            avgRate = (avgRate + increment/interval)/2
          }
          ts = ar(i*3+4).trim().toLong
          tslevel = ar(i*3+5).trim().toDouble
        }
      }
    }
    (ts, avgRate, tslevel)
  }
  
  def getEta(dataTuple: (String, (Long, Double, Double)), highWaterMark: Double): Long = {
    INFO(f"Calculate ETA iff ${dataTuple._2._2} > 0 and ${highWaterMark} > ${dataTuple._2._3}")
    var eta: Long = -1L
    if (dataTuple._2._2 > 0.0 && highWaterMark > dataTuple._2._3) {
      eta = Math.round(Math.floor((highWaterMark-dataTuple._2._3)/dataTuple._2._2))
    } else if (dataTuple._2._2>=0.0 && dataTuple._2._3 >= highWaterMark) {
      eta = 0L
    }
    eta
  }

  def stringize(record: String): String= {
    var json = JSON.parseRaw(record).getOrElse(null)
    var result=""
    if (json!=null) {
      val map = json.asInstanceOf[JSONObject].obj.asInstanceOf[Map[String, String]]
      result = s"${map(TANKID)},${map(TS)},${map(WATERTANKLEVEL)}"
    }
    result
  }

  def INFO(s: String): Unit={
    if (log.isInfoEnabled()) {
      log.info(s)
    }
  }
  
  def ERROR(s: String): Unit={
      log.error(s)
  }
  
}