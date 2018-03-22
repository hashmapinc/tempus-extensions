package com.hashmapinc.tempus.kafka.producer;

import com.hashmapinc.tempus.common.Message;
import com.hashmapinc.tempus.kafka.consumer.KafkaConsumerContext;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class TestKafkaStreamProducer {
    MockProducer<String, String> producer;
    KafkaStreamProducer kafkaStreamProducer;

    @Before
    public void setUp() {
        KafkaProducerContext kafkaConsumerContext = new KafkaProducerContext("0.0.0.0", 9092, "test");;
        producer = new MockProducer<String, String>(true, new StringSerializer(), new StringSerializer());
        kafkaStreamProducer = new KafkaStreamProducer(kafkaConsumerContext, producer);
    }

    @Test
    public void testProducer() throws IOException {
        Message message = new Message("{\"key1\":\"value1\", \"key2\":\"value2\"}");
        kafkaStreamProducer.send(message);

        List<ProducerRecord<String, String>> history = producer.history();

        List<ProducerRecord<String, String>> expected = Arrays.asList(new ProducerRecord<String, String>("test", "{\"key1\":\"value1\", \"key2\":\"value2\"}"));
        assertNotNull(history);
        assertEquals(expected, history);
    }
}
