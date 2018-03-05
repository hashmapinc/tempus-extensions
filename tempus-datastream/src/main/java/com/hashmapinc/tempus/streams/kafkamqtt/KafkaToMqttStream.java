package com.hashmapinc.tempus.streams.kafkamqtt;

import com.hashmapinc.tempus.kafka.consumer.KafkaConsumerContext;
import com.hashmapinc.tempus.kafka.config.KafkaDefaultConfig;
import com.hashmapinc.tempus.kafka.consumer.KafkaStreamConsumer;
import com.hashmapinc.tempus.kafka.handler.MqttStreamHandler;
import com.hashmapinc.tempus.exception.PublishMsgException;
import com.hashmapinc.tempus.exception.StreamCreationException;
import com.hashmapinc.tempus.mqtt.client.MqttBrokerClient;
import com.hashmapinc.tempus.mqtt.client.MqttContext;
import com.hashmapinc.tempus.mqtt.exception.MqttClientException;

public class KafkaToMqttStream {
    private KafkaStreamConsumer kafkaStreamConsumer;
    private MqttBrokerClient mqttBrokerClient;
    private int kafkaPollingInterval;
    private MqttStreamHandler msgHandler;

    public void createKafkaToMqttStream(KafkaConsumerContext kafkaConsumerContext, MqttContext mqttContext) throws StreamCreationException, PublishMsgException {
        this.kafkaStreamConsumer = new KafkaStreamConsumer(kafkaConsumerContext);
        this.kafkaPollingInterval = KafkaDefaultConfig.POLLING_INTERVAL_MS_CONFIG;
        initMqttBrokerClient(mqttContext);
        msgHandler = new MqttStreamHandler(mqttBrokerClient);
        kafkaStreamConsumer.startStream(msgHandler, kafkaPollingInterval);
    }

    public void createKafkaToMqttStream(KafkaConsumerContext kafkaConsumerContext, MqttContext mqttContext, int kafkaPollingInterval) throws StreamCreationException, PublishMsgException {
        this.kafkaStreamConsumer = new KafkaStreamConsumer(kafkaConsumerContext);
        initMqttBrokerClient(mqttContext);
        this.kafkaPollingInterval = kafkaPollingInterval;
        msgHandler = new MqttStreamHandler(mqttBrokerClient);
        kafkaStreamConsumer.startStream(msgHandler, kafkaPollingInterval);
    }

    private void initMqttBrokerClient(MqttContext mqttContext) throws StreamCreationException {
        try {
            this.mqttBrokerClient = new MqttBrokerClient(mqttContext);
        } catch (MqttClientException ex) {
            throw new StreamCreationException(ex);
        }
    }
}
