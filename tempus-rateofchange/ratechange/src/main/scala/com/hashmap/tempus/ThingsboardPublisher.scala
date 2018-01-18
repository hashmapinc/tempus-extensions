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

import org.eclipse.paho.client.mqttv3.{MqttConnectOptions,MqttAsyncClient,IMqttActionListener,MqttMessage,MqttException,IMqttToken}
import java.nio.charset.StandardCharsets
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode

import org.apache.log4j.Logger
import org.apache.log4j.Level

object ThingsboardPublisher {
  val log = Logger.getLogger(ThingsboardPublisher.getClass)
  
  val GATEWAY_ACCESS_TOKEN: String = "GATEWAY_ACCESS_TOKEN"
  val MQTT_TOPIC: String = "v1/gateway/telemetry"

   def connectToThingboard(mqttUrl: String):MqttAsyncClient ={
    INFO(s"trying to connect to ${mqttUrl}")
    var client = new MqttAsyncClient(mqttUrl, MqttAsyncClient.generateClientId())
    var options = new MqttConnectOptions()
    options.setUserName(GATEWAY_ACCESS_TOKEN)
    client.connect(options, null, new IMqttActionListener{
      def onFailure(x1: IMqttToken,x2: Throwable): Unit ={}
      def onSuccess(x1: IMqttToken): Unit = {}
    }).waitForCompletion()
    
    client
  }
  
  def publishTelemetryToThingsboard(client: MqttAsyncClient, dataTuple: (String, (Long, Double, Double)), highWaterMark: Double, eta: Long): Unit = {
    INFO(f"Publish telemetry called with tankid=${dataTuple._1}, ts=${dataTuple._2._1}, fillspeed=${dataTuple._2._2}, level=${dataTuple._2._3}, highmark=${highWaterMark}, and eta=${eta}")
    if (dataTuple._1.trim().length()>0) {
      val dataMsg = new MqttMessage(toDataJson(dataTuple._1, dataTuple._2._1, eta).getBytes(StandardCharsets.UTF_8));
      INFO(s"Publishing to thingsboard: ${dataMsg}")
      client.publish(MQTT_TOPIC, dataMsg, null, getCallBack());
      INFO("After publishing to thingsboard")
    } else {
      ERROR(s"Received request to publish data for unknown tankId=<${dataTuple._1}>. Data will not be published to mqtt.")
    }
  }
  
  def getCallBack(): IMqttActionListener = {
     new IMqttActionListener {
                def onSuccess(asyncActionToken: IMqttToken): Unit= {}
                def onFailure(asyncActionToken: IMqttToken, failureException: Throwable) {}
            }
  }
  
  def toDataJson(tankId: String, ts: Long, timeToFill: Long): String = {
    val mapper = new ObjectMapper()
    val json = mapper.createObjectNode()
    var ja = json.putArray("Tank "+tankId)
    var obj=ja.addObject()
    obj.put("ts", ts)
    obj=obj.putObject("values")
    obj.put("timeToFill", timeToFill)
    mapper.writeValueAsString(json)
  }
  
  def disconnect(client: MqttAsyncClient): Unit={
    client.disconnect()
  }
  
  def INFO(s: String): Unit={
    if (log.isInfoEnabled()) {
      log.info(s)
    }
  }
  
  def ERROR(s: String): Unit={
      log.error(s)
  }
  
  def setLogLevel(level: Level): Unit={
    log.setLevel(level)
  }
}