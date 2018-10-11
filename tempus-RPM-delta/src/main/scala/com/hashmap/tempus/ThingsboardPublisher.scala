package com.hashmap.tempus

import java.nio.charset.StandardCharsets

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.log4j.{Level, Logger}
import org.eclipse.paho.client.mqttv3._

object ThingsboardPublisher {
  private val log = Logger.getLogger(ThingsboardPublisher.getClass)
  
  val GATEWAY_ACCESS_TOKEN: String = "GATEWAY_ACCESS_TOKEN"
  val MQTT_TOPIC: String = "v1/gateway/telemetry"

   def connectToThingboard(mqttUrl: String, gatewayToken: String):MqttAsyncClient ={
    INFO(s"trying to connect to $mqttUrl")
    var client = new MqttAsyncClient(mqttUrl, MqttAsyncClient.generateClientId())
    var options = new MqttConnectOptions()
    options.setUserName(gatewayToken)
    client.connect(options, null, new IMqttActionListener{
      def onFailure(x1: IMqttToken,x2: Throwable): Unit ={}
      def onSuccess(x1: IMqttToken): Unit = {}
    }).waitForCompletion()
    
    client
  }
  
  def publishTelemetryToThingsboard(client: MqttAsyncClient, dataTuple: (Long, Double)): Unit = {
    INFO(f"Publish telemetry called with ts=${dataTuple._1}, rpmDelta=${dataTuple._2}")
    val dataMsg = new MqttMessage(toDataJson(dataTuple._1, dataTuple._2).getBytes(StandardCharsets.UTF_8))
    INFO(s"Publishing to thingsboard: $dataMsg")
    client.publish(MQTT_TOPIC, dataMsg, null, getCallBack)
    INFO("After publishing to thingsboard")
  }
  
  def getCallBack: IMqttActionListener = {
     new IMqttActionListener {
                def onSuccess(asyncActionToken: IMqttToken): Unit= {}
                def onFailure(asyncActionToken: IMqttToken, failureException: Throwable) {}
            }
  }
  
  def toDataJson(ts: Long, rpmDelta: Double): String = {
    val mapper = new ObjectMapper()
    val data = Map("ts" -> ts, "rpmDelta" -> rpmDelta)
    mapper.writeValueAsString(data)
  }
  
  def disconnect(client: MqttAsyncClient): Unit={
    client.disconnect()
  }
  
  def INFO(s: String): Unit={
    if (log.isInfoEnabled) {
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