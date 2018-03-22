package com.hashmapinc.tempus.mqtt.client;

import com.hashmapinc.tempus.common.Message;
import com.hashmapinc.tempus.mqtt.config.MqttQos;
import com.hashmapinc.tempus.mqtt.config.MqttTbTopic;
import com.hashmapinc.tempus.mqtt.exception.MqttClientException;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;

import java.util.UUID;

public class MqttBrokerClient {
    private MqttClient mqttClient;
    private MqttTbTopic topic;
    private MqttConnectOptions options;

    public MqttBrokerClient(MqttContext mqttContext) throws MqttClientException {
        this.options = mqttContext.getMqttConnectOptions();
        this.topic = mqttContext.getTbTopic();
        String brokerUrl = mqttContext.getBrokerUrl();
        this.mqttClient = createMqttClient(brokerUrl);
        connectClient();
    }

    private MqttClient createMqttClient(String brokerUrl) throws MqttClientException {
        try {
            return new MqttClient(brokerUrl, UUID.randomUUID().toString());
        } catch (MqttException ex) {
            throw new MqttClientException(ex);
        }
    }

    private void connectClient() throws MqttClientException {
        try {
            mqttClient.connect(options);
        } catch (MqttException ex) {
            ex.printStackTrace();
            throw new MqttClientException(ex);
        }
    }

    private void disconnectClient() throws MqttClientException {
        try {
            mqttClient.disconnect();
        } catch (MqttException ex) {
            throw new MqttClientException(ex);
        }
    }

    public void publish(Message message) throws MqttClientException {
        MqttMessage mqttMessage = createMqttMessage(message.getPayload());
        publishMessage(mqttMessage);
    }

    private MqttMessage createMqttMessage(String payload) {
        MqttMessage mqttMessage = new MqttMessage(payload.getBytes());
        mqttMessage.setQos(MqttQos.QOS1.getValue());
        return mqttMessage;
    }

    private void publishMessage(MqttMessage mqttMessage) throws MqttClientException {
        try {
            mqttClient.publish(topic.toString(), mqttMessage);
        } catch (MqttException ex) {
            throw new MqttClientException(ex);
        }
    }

    public void subscribe() throws MqttClientException{
        try {
            mqttClient.subscribe(topic.getTbTopic(), MqttQos.QOS0.getValue());
        } catch (MqttException ex) {
            throw new MqttClientException(ex);
        }
    }
}
