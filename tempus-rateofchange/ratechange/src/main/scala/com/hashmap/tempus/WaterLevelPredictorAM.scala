package com.hashmap.tempus

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

import org.apache.spark.streaming._
import org.apache.spark.{SparkConf,SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._

//import org.eclipse.paho.client.mqttv3.{MqttConnectOptions,MqttAsyncClient,IMqttActionListener,MqttMessage,MqttException,IMqttToken}
//import java.nio.charset.StandardCharsets
//import com.fasterxml.jackson.databind.ObjectMapper
//import com.fasterxml.jackson.databind.node.ObjectNode

import org.apache.log4j.Logger
import org.apache.log4j.Level

import org.apache.spark.mllib.linalg.{Vector, Vectors}
import com.cloudera.sparkts.models.{ARIMA, ARIMAModel}

object WaterLevelPredictorAM {
  val log = Logger.getLogger(WaterLevelPredictorAM.getClass)
  
  def main(args: Array[String]): Unit = {
    if (args.length<6) {
      assert(args.length>=6, "Usage: mqttUrl kafkaUrl kafkaTopic tankId highWaterMark windowSize optionalDebugLevel.\nTry mqttyUrl as tcp://tb:1883, kafkaUrl as kafka:9092, topic as water-tank-level-data, tankId as 123, highWaterMark as 70.0, windowSize as 1, and debuglevel as DEBUG=INFO")
    }
    if (args.length>6) {
      val level = args(6).split("=")(1).trim()
      if (level.equalsIgnoreCase("INFO"))
        log.setLevel(Level.INFO)
        ThingsboardPublisher.setLogLevel(Level.INFO)
    }
    val mqttUrl = args(0)
    val kafkaParams = Map[String, Object](
     "bootstrap.servers" -> args(1), //"kafka:9092",
     "key.deserializer" -> classOf[StringDeserializer],
     "value.deserializer" -> classOf[StringDeserializer],
     "group.id" -> "DEFAULT_GROUP_ID",
     "auto.offset.reset" -> "latest",
     "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    val topics = Array(args(2)) 
    val tankId = args(3)
    val highWaterMark = args(4).trim().toDouble
    val winSize = Minutes(Integer.parseInt(args(5)))
    val batchSize = winSize //Seconds(20)

    val sparkConf = new SparkConf().setAppName("WaterLevelPredictorArimaModel")
    val ssc = new StreamingContext(sparkConf, batchSize)
    ssc.sparkContext.setLogLevel("WARN")
    val sqlContext = new SQLContext(ssc.sparkContext)
    val spark = SparkSession
                  .builder()
                  .appName("WaterLevelPredictorArimaModel")
                  .getOrCreate()
    import spark.implicits._

    
    val stream = KafkaUtils
                    .createDirectStream[String, String](ssc, PreferConsistent, Subscribe[String, String](topics, kafkaParams))
    
    assert((stream != null), ERROR("Kafka stream is not available. Check your Kafka setup."))
    
    val values = stream.map(record => record.value())
    val records = values.transform(rdd=>{
        val df = sqlContext.read.json(rdd)
        df.createOrReplaceTempView("wtd")
        var trdd: RDD[String] = ssc.sparkContext.parallelize(Nil)
        if (!rdd.isEmpty()) {
          trdd = spark.sql("select concat(tankId, ',', ts, ',', waterTankLevel) as rec from wtd where tankId='"+tankId+"'").rdd.map(r=>r.toString())
        }
        INFO("Contents of trdd is set with "+trdd.count()+" records")
        trdd
      })
      
    //At this point in time, records is expected to have 0 or more elements
    //in each RDD within the window for the specified tankId and so let us consolidate
    //NOTE: records could be empty if there was no data for the specified tankId
    val result = records.map(r => r.replace("[", "").replace("]", ""))
                        .map(r => (r.split(",")(0), r.split(",")(2)))
                        .reduceByKeyAndWindow((r1:String, r2:String)=>if (r1.length()>0 && r2.length()>0) r1+","+r2 else r1+r2, winSize, winSize)
    
    //At this point in time, result is expected to have 0 or 1 record in a single RDD
    //var theClient:MqttAsyncClient = null
    result.foreachRDD(rdd =>{
      if (!rdd.isEmpty()) {
        //In real-life, the model will have been constructed in the beginning
        //but for now, let us create a model and then forecast
        val vector: Vector = Vectors.dense(getWaterlevelValues(rdd.map(r=>r._2).collect()))
        if (model==null || !(model.isStationary() && model.isInvertible())) {
          INFO("Fitting the model now")
          model = ARIMA.fitModel(Integer.parseInt(args(5)), 1, 2, vector)
        } else {INFO("Using existing stationary and invertible model")}
        val forecast = model.forecast(vector, 5)
        INFO("FORECAST SIZE="+forecast.size)
        INFO("FORECAST: "+forecast.toArray.mkString(","))
      }
    })
  
    ssc.start()
    ssc.awaitTermination()
  }  
  
  def getWaterlevelValues(s:Array[String]): Array[Double] = {
    INFO("CONSOLIDATED REC SIZE="+s.length)
    if (s.length == 0) return null
    INFO("INPUT IS: "+s(0))
    val ar = s(0).replace("[","").replace("]","").split(",")
    INFO("REC COUNT: "+ar.length)
    var dv: Array[Double]=new Array[Double](ar.length)
    for (i <- 0 until ar.length) {
      dv(i)=ar(i).toDouble
    }
    dv
  }
  
  def INFO(s: String): Unit={
    if (log.isInfoEnabled()) {
      log.info(s)
    }
  }
  
  def ERROR(s: String): Unit={
      log.error(s)
  }
  
  var model: ARIMAModel = null
}