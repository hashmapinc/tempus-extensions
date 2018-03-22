package com.hashmapinc.tempus.kafka.producer;

import com.hashmapinc.tempus.kafka.config.KafkaDefaultConfig;

import java.util.Properties;

public class KafkaProducerContext {
    private Properties kafkaProducerProperties;
    private String kafkaTopic;

    public KafkaProducerContext(String kafkaServerIp, int kafkaServerPort, String kafkaTopic) {
        this.kafkaProducerProperties = new Properties();
        this.kafkaTopic = kafkaTopic;
        putKafkaDefaultProperties(kafkaServerIp, kafkaServerPort);
    }

    public KafkaProducerContext(String kafkaServerIp, int kafkaServerPort, String kafkaTopic, Properties properties) {
        this.kafkaProducerProperties = new Properties(properties);
        this.kafkaTopic = kafkaTopic;
        putKafkaDefaultProperties(kafkaServerIp, kafkaServerPort);
    }

    private void putKafkaDefaultProperties(String kafkaServerIp, int kafkaServerPort) {
        kafkaProducerProperties.put("bootstrap.servers", kafkaServerIp + ":" + Integer.toString(kafkaServerPort));
        kafkaProducerProperties.putIfAbsent("client.id", KafkaDefaultConfig.KAFKA_CLIENT_ID_CONFIG);
        kafkaProducerProperties.putIfAbsent("key.serializer", KafkaDefaultConfig.KEY_SERIALIZER_CLASS_CONFIG);
        kafkaProducerProperties.putIfAbsent("value.serializer", KafkaDefaultConfig.VALUE_SERIALIZER_CLASS_CONFIG);
        kafkaProducerProperties.putIfAbsent("acks", KafkaDefaultConfig.KAFKA_PRODUCER_ACKS_CONFIG);
        kafkaProducerProperties.putIfAbsent("retries", KafkaDefaultConfig.KAFKA_PRODUCER_RETRIES_CONFIG);
    }

    public String getKafkaTopic() {
        return kafkaTopic;
    }

    public Properties getKafkaProducerProperties() {
        return kafkaProducerProperties;
    }
}
