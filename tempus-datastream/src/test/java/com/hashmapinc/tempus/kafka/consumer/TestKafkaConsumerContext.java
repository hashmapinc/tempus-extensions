package com.hashmapinc.tempus.kafka.consumer;

import com.hashmapinc.tempus.kafka.config.KafkaDefaultConfig;
import com.hashmapinc.tempus.kafka.consumer.KafkaConsumerContext;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.junit.Test;

import java.util.Properties;

import static org.junit.Assert.assertEquals;


public class TestKafkaConsumerContext {

    @Test
    public void testKafkaConsumerContextWithoutProperties() {
        Properties defaultProperties = new Properties();
        defaultProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "0.0.0.0" + ":" + Integer.toString(9092));
        defaultProperties.putIfAbsent(ConsumerConfig.GROUP_ID_CONFIG, KafkaDefaultConfig.KAFKA_CLIENT_ID_CONFIG);
        defaultProperties.putIfAbsent(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, KafkaDefaultConfig.ENABLE_AUTO_COMMIT_CONFIG);
        defaultProperties.putIfAbsent(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, KafkaDefaultConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG);
        defaultProperties.putIfAbsent(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, KafkaDefaultConfig.SESSION_TIMEOUT_MS_CONFIG);
        defaultProperties.putIfAbsent(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KafkaDefaultConfig.KEY_DESERIALIZER_CLASS_CONFIG);
        defaultProperties.putIfAbsent(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaDefaultConfig.VALUE_DESERIALIZER_CLASS_CONFIG);

        KafkaConsumerContext kafkaConsumerContext = new KafkaConsumerContext("0.0.0.0", 9092, "test");

        assertEquals(defaultProperties, kafkaConsumerContext.getKafkaConsumerProperties());
        assertEquals("test", kafkaConsumerContext.getKafkaTopic());
    }

    @Test
    public void testKafkaConsumerContextWithProperties() {
        Properties defaultProperties = new Properties();
        defaultProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "0.0.0.0" + ":" + Integer.toString(9092));
        defaultProperties.putIfAbsent(ConsumerConfig.GROUP_ID_CONFIG, KafkaDefaultConfig.KAFKA_CLIENT_ID_CONFIG);
        defaultProperties.putIfAbsent(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, KafkaDefaultConfig.ENABLE_AUTO_COMMIT_CONFIG);
        defaultProperties.putIfAbsent(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, KafkaDefaultConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG);
        defaultProperties.putIfAbsent(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, KafkaDefaultConfig.SESSION_TIMEOUT_MS_CONFIG);
        defaultProperties.putIfAbsent(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KafkaDefaultConfig.KEY_DESERIALIZER_CLASS_CONFIG);
        defaultProperties.putIfAbsent(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaDefaultConfig.VALUE_DESERIALIZER_CLASS_CONFIG);

        KafkaConsumerContext kafkaConsumerContext = new KafkaConsumerContext("0.0.0.0", 9092, "test", new Properties());

        assertEquals(defaultProperties, kafkaConsumerContext.getKafkaConsumerProperties());
        assertEquals("test", kafkaConsumerContext.getKafkaTopic());
    }
}
