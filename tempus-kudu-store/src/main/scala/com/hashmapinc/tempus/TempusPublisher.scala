package com.hashmapinc.tempus

import java.nio.charset.StandardCharsets

import com.hashmapinc.util.{TempusKuduConstants, TempusUtils}
import org.apache.log4j.Logger
import org.eclipse.paho.client.mqttv3.{IMqttActionListener, IMqttToken, MqttAsyncClient, MqttConnectOptions, MqttMessage}

object TempusPublisher {
  val log = Logger.getLogger(TempusPublisher.getClass)


  def connect(mqttUrl: String, gatewayToken: String):MqttAsyncClient ={
    log.info(s"trying to connect to ${mqttUrl}")
    //System.out.println(s"trying to connect to ${mqttUrl}")
    var client = new MqttAsyncClient(mqttUrl, MqttAsyncClient.generateClientId(),null)
    var options = new MqttConnectOptions()
    options.setMaxInflight(TempusKuduConstants.MAX_INFLIGHT_SIZE)
    options.setUserName(gatewayToken)


   // options.setAutomaticReconnect(true)

    client.connect(options, null, new IMqttActionListener{
      def onFailure(x1: IMqttToken,x2: Throwable): Unit ={ log.info(s" onFailure  ${mqttUrl}")
       // System.out.println(s" onFailure  ${mqttUrl}")
      }
      def onSuccess(x1: IMqttToken): Unit = {log.info(s" onSuccess  ${mqttUrl}")
       // System.out.println(s" onSuccess  ${mqttUrl}")
      }
    }).waitForCompletion()

    client
  }

  def publishMSE(client: MqttAsyncClient , mqttTopic: String, data: String): Unit = {
    if (data.size>0) {
     //   val client = TempusPublisher.connect(mqttUrl, gatewayToken)
      val dataMsg = new MqttMessage(data.getBytes(StandardCharsets.UTF_8));
      client.publish(mqttTopic, dataMsg, null, getCallBack());
      log.error("After publishing to tempus")
    } else {
      log.error(s"Received request to publish insufficient data=<${data}>.")
    }
  }

  def getCallBack(): IMqttActionListener = {
    new IMqttActionListener {
      def onSuccess(asyncActionToken: IMqttToken): Unit= {}
      def onFailure(asyncActionToken: IMqttToken, failureException: Throwable) {}
    }
  }

  def disconnect(client: MqttAsyncClient): Unit={
    client.disconnect()
  }




}
