package com.hashmapinc.tempus.kafka.consumer;

import com.hashmapinc.tempus.common.Message;
import com.hashmapinc.tempus.exception.PublishMsgException;
import com.hashmapinc.tempus.handler.BaseStreamHandler;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

public class KafkaStreamConsumer {
    private Consumer kafkaConsumer;
    private BaseStreamHandler kafkaStreamHandler;
    private String kafkaTopic;

    public KafkaStreamConsumer(KafkaConsumerContext kafkaConsumerContext) {
        Properties kafkaConsumerProperties = kafkaConsumerContext.getKafkaConsumerProperties();
        kafkaConsumer = new KafkaConsumer<>(kafkaConsumerProperties);
        kafkaTopic = kafkaConsumerContext.getKafkaTopic();
    }

    public KafkaStreamConsumer(KafkaConsumerContext kafkaConsumerContext, Consumer kafkaConsumer) {
        Properties kafkaConsumerProperties = kafkaConsumerContext.getKafkaConsumerProperties();
        this.kafkaConsumer = kafkaConsumer;
        kafkaTopic = kafkaConsumerContext.getKafkaTopic();
    }

    public void startStream(BaseStreamHandler kafkaStreamHandler, int pollingInterval) throws PublishMsgException {
        this.kafkaStreamHandler = kafkaStreamHandler;
        startPolling(pollingInterval);
    }

    private void startPolling(int pollingInterval) throws PublishMsgException {
        kafkaConsumer.subscribe(Arrays.asList(kafkaTopic));
        pollKafkaTopic(pollingInterval);
    }

    private void pollKafkaTopic(int pollingInterval) throws PublishMsgException {
        while (true) {
            ConsumerRecords<String, String> records = kafkaConsumer.poll(pollingInterval);
            for(ConsumerRecord<String, String> record : records) {
                pushMsgToHandler(new Message(record.value()));
            }
        }
    }

    private void pushMsgToHandler(Message msg) throws PublishMsgException {
        try {
            kafkaStreamHandler.onMessage(msg);
        } catch (PublishMsgException ex) {
            throw new PublishMsgException(ex);
        }
    }
}
