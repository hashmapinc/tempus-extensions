package com.hashmapinc.tempus.kafka.handler;

import com.hashmapinc.tempus.common.Message;
import com.hashmapinc.tempus.exception.PublishMsgException;
import com.hashmapinc.tempus.handler.BaseStreamHandler;
import com.hashmapinc.tempus.mqtt.client.MqttBrokerClient;
import com.hashmapinc.tempus.mqtt.exception.MqttClientException;

public class MqttStreamHandler implements BaseStreamHandler {
    private MqttBrokerClient mqttBrokerClient;

    public MqttStreamHandler(MqttBrokerClient mqttBrokerClient) {
        this.mqttBrokerClient = mqttBrokerClient;
    }

    @Override
    public void onMessage(Message message) throws PublishMsgException{
        try {
            mqttBrokerClient.publish(message);
        } catch (MqttClientException ex) {
            throw new PublishMsgException(ex);
        }
    }
}
