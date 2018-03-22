package com.hashmapinc.tempus.kafka.consumer;

import com.hashmapinc.tempus.exception.PublishMsgException;

public class KafkaConsumerThread extends Thread {
    public KafkaStreamConsumer kafkaStreamConsumer;
    public TestStreamHandler testStreamHandler;

    public KafkaConsumerThread(KafkaStreamConsumer kafkaStreamConsumer, TestStreamHandler testStreamHandler) {
        this.kafkaStreamConsumer = kafkaStreamConsumer;
        this.testStreamHandler = testStreamHandler;
    }

    @Override
    public void run() {
        try {
            kafkaStreamConsumer.startStream(testStreamHandler, 100);
        } catch (PublishMsgException ex) {
            ex.printStackTrace();
        }
    }
}
