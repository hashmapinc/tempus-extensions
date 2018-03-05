package com.hashmapinc.tempus.mqtt.handler;

import com.hashmapinc.tempus.common.Message;
import com.hashmapinc.tempus.kafka.producer.KafkaStreamProducer;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttMessage;

public class MqttCallbackHandler implements MqttCallback {
    KafkaStreamProducer kafkaStreamProducer;

    public MqttCallbackHandler(KafkaStreamProducer kafkaStreamProducer) {
        this.kafkaStreamProducer = kafkaStreamProducer;
    }

    @Override
    public void connectionLost(Throwable throwable) {

    }

    @Override
    public void deliveryComplete(IMqttDeliveryToken iMqttDeliveryToken) {

    }

    @Override
    public void messageArrived(String s, MqttMessage mqttMessage) throws Exception {
        Message message = new Message(mqttMessage.getPayload().toString());
        System.out.println("Message : " + message.getPayload());
        kafkaStreamProducer.send(message);
    }
}
