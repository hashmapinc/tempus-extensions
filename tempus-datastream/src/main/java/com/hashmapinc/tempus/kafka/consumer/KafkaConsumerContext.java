package com.hashmapinc.tempus.kafka.consumer;

import com.hashmapinc.tempus.kafka.config.KafkaDefaultConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

public class KafkaConsumerContext {
    private Properties kafkaConsumerProperties;
    private String kafkaTopic;

    public KafkaConsumerContext(String kafkaServerIp, int kafkaServerPort, String kafkaTopic) {
        this.kafkaConsumerProperties = new Properties();
        this.kafkaTopic = kafkaTopic;
        putKafkaDefaultProperties(kafkaServerIp, kafkaServerPort);
    }

    public KafkaConsumerContext(String kafkaServerIp, int kafkaServerPort, String kafkaTopic, Properties properties) {
        this.kafkaConsumerProperties = new Properties(properties);
        this.kafkaTopic = kafkaTopic;
        putKafkaDefaultProperties(kafkaServerIp, kafkaServerPort);
    }

    private void putKafkaDefaultProperties(String kafkaServerIp, int kafkaServerPort) {
        kafkaConsumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServerIp + ":" + Integer.toString(kafkaServerPort));
        kafkaConsumerProperties.putIfAbsent(ConsumerConfig.GROUP_ID_CONFIG, KafkaDefaultConfig.KAFKA_CLIENT_ID_CONFIG);
        kafkaConsumerProperties.putIfAbsent(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, KafkaDefaultConfig.ENABLE_AUTO_COMMIT_CONFIG);
        kafkaConsumerProperties.putIfAbsent(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, KafkaDefaultConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG);
        kafkaConsumerProperties.putIfAbsent(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, KafkaDefaultConfig.SESSION_TIMEOUT_MS_CONFIG);
        kafkaConsumerProperties.putIfAbsent(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KafkaDefaultConfig.KEY_DESERIALIZER_CLASS_CONFIG);
        kafkaConsumerProperties.putIfAbsent(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaDefaultConfig.VALUE_DESERIALIZER_CLASS_CONFIG);
    }

    public String getKafkaTopic() {
        return kafkaTopic;
    }

    public Properties getKafkaConsumerProperties() {
        return kafkaConsumerProperties;
    }
}
