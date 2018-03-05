package com.hashmapinc.tempus.kafka.producer;

import com.hashmapinc.tempus.common.Message;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class KafkaStreamProducer {
    private Producer kafkaProducer;
    private String kafkaTopic;

    public KafkaStreamProducer(KafkaProducerContext kafkaProducerContext) {
        Properties kafkaProducerProperties = kafkaProducerContext.getKafkaProducerProperties();
        kafkaProducer = new KafkaProducer<>(kafkaProducerProperties);
        kafkaTopic = kafkaProducerContext.getKafkaTopic();
    }

    public KafkaStreamProducer(KafkaProducerContext kafkaProducerContext, Producer producer) {
        Properties kafkaProducerProperties = kafkaProducerContext.getKafkaProducerProperties();
        kafkaProducer = producer;
        kafkaTopic = kafkaProducerContext.getKafkaTopic();
    }

    public void send(Message message) {
        kafkaProducer.send(new ProducerRecord<String, String>(kafkaTopic, message.getPayload()));
    }
}
