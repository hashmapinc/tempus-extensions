package com.hashmap.tempus

import java.nio.charset.StandardCharsets

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.log4j.{Level, Logger}
import org.eclipse.paho.client.mqttv3._

object ThingsboardPublisher {
  private val log = Logger.getLogger(ThingsboardPublisher.getClass)
  
  val GATEWAY_ACCESS_TOKEN: String = "GATEWAY_ACCESS_TOKEN"
  val MQTT_TOPIC: String = "v1/gateway/telemetry"

   def connectToThingsBoard(mqttUrl: String, gatewayToken: String):MqttAsyncClient ={
    INFO(s"trying to connect to $mqttUrl")
    val client = new MqttAsyncClient(mqttUrl, MqttAsyncClient.generateClientId())
    val options = new MqttConnectOptions()
    options.setUserName(gatewayToken)
    client.connect(options, null, new IMqttActionListener{
      def onFailure(x1: IMqttToken,x2: Throwable): Unit ={}
      def onSuccess(x1: IMqttToken): Unit = {}
    }).waitForCompletion()
    
    client
  }
  
  def publishTelemetryToThingsBoard(client: MqttAsyncClient, maxRpm: Double, minRpm: Double, delta: Double, deviceId: String): Unit = {
    INFO(f"Publish telemetry for stick slick rpm data called with maxRpm=$maxRpm, minRpm=$minRpm, maxMinDelta=$delta")
    val dataMsg = new MqttMessage(toDataJson(maxRpm, minRpm, delta, deviceId).getBytes(StandardCharsets.UTF_8))
    INFO(s"Publishing to thingsboard: $dataMsg")
    client.publish(MQTT_TOPIC, dataMsg, null, getCallBack)
  }
  
  def getCallBack: IMqttActionListener = {
     new IMqttActionListener {
                def onSuccess(asyncActionToken: IMqttToken): Unit= {}
                def onFailure(asyncActionToken: IMqttToken, failureException: Throwable) {}
            }
  }
  
  def toDataJson(maxRpm: Double, minRpm: Double, delta: Double, deviceId: String): String = {
    val mapper = new ObjectMapper()
    val json = mapper.createObjectNode()
    val ja = json.putArray("Tank " + deviceId)
    var obj=ja.addObject()
    obj.put("ts", System.currentTimeMillis())
    obj=obj.putObject("values")
    obj.put("RPMP2PMAX", maxRpm)
    obj.put("RPMP2PMIN", minRpm)
    obj.put("RPMP2P", delta)
    mapper.writeValueAsString(json)
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