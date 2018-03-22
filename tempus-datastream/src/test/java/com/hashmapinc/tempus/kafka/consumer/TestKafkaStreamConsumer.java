package com.hashmapinc.tempus.kafka.consumer;

import com.hashmapinc.tempus.common.Message;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.junit.Before;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class TestKafkaStreamConsumer {

    KafkaStreamConsumer kafkaStreamConsumer;
    TestStreamHandler testStreamHandler;
    MockConsumer<String, String> consumer;

    @Before
    public void setup() {
        consumer = new MockConsumer<String, String>(OffsetResetStrategy.EARLIEST);
        KafkaConsumerContext kafkaConsumerContext = new KafkaConsumerContext("0.0.0.0", 9092, "test");;
        kafkaStreamConsumer = new KafkaStreamConsumer(kafkaConsumerContext, consumer);
        testStreamHandler = new TestStreamHandler();
    }

    @Test
    public void startStream() throws InterruptedException {
        Collection<TopicPartition> partitions = new ArrayList<TopicPartition>();
        partitions.add(new TopicPartition("test", 0));
        consumer.subscribe(Arrays.asList("test"));
        consumer.rebalance(partitions);

        HashMap<TopicPartition, Long> beginningOffsets = new HashMap<>();
        beginningOffsets.put(new TopicPartition("test", 0), 0L);

        consumer.updateBeginningOffsets(beginningOffsets);

        consumer.addRecord(new ConsumerRecord<String, String>("test",
                0, 0L, "key", "value0"));
        consumer.addRecord(new ConsumerRecord<String, String>("test", 0,
                1L, "key", "value1"));


        Thread t1 = new KafkaConsumerThread(kafkaStreamConsumer, testStreamHandler);
        t1.start();
        Thread.sleep(1000);

        ArrayList<Message> expectedMessage = new ArrayList<>();
        expectedMessage.add(new Message("value0"));
        expectedMessage.add(new Message("value1"));

        assertNotNull(testStreamHandler.getMessages());
        assertEquals(expectedMessage.get(0).getPayload(), testStreamHandler.getMessages().get(0).getPayload());
    }
}
