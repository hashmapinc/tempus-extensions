package com.hashmapinc.tempus.mqtt;

import com.hashmapinc.tempus.common.Message;
import com.hashmapinc.tempus.mqtt.client.MqttBrokerClient;
import com.hashmapinc.tempus.mqtt.client.MqttContext;
import com.hashmapinc.tempus.mqtt.config.MqttTbTopic;
import com.hashmapinc.tempus.mqtt.exception.MqttClientException;
import io.moquette.BrokerConstants;
import io.moquette.interception.AbstractInterceptHandler;
import io.moquette.interception.InterceptHandler;
import io.moquette.interception.messages.InterceptPublishMessage;
import io.moquette.interception.messages.InterceptSubscribeMessage;
import io.moquette.server.Server;
import io.moquette.server.config.ClasspathConfig;
import io.moquette.server.config.IConfig;
import io.moquette.server.config.MemoryConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static org.junit.Assert.assertEquals;

public class TestMqttBrokerClient {

    IConfig classPathConfig;
    Server mqttBroker;
    List<? extends InterceptHandler> userHandlers;
    MqttContext mqttContext;
    MqttBrokerClient mqttBrokerClient;
    ArrayList<Message> messages;
    String topic;

    private class PublisherListener extends AbstractInterceptHandler {
        @Override
        public void onPublish(InterceptPublishMessage message) {
            messages.add(new Message(new String(message.getPayload().array())));
        }

        @Override
        public void onSubscribe(InterceptSubscribeMessage message) {
            topic = message.getTopicFilter();
        }
    }

    private void startBroker() throws IOException {
        final Properties configProps = new Properties();
        configProps.put(BrokerConstants.ALLOW_ANONYMOUS_PROPERTY_NAME, "true");
        configProps.put(BrokerConstants.WEB_SOCKET_PORT_PROPERTY_NAME, "1884");
        classPathConfig = new MemoryConfig(configProps);
        mqttBroker = new Server();
        userHandlers = Arrays.asList(new PublisherListener());
        mqttBroker.startServer(classPathConfig, userHandlers);
    }

    @Before
    public void setup() throws IOException, InterruptedException, MqttClientException{
        startBroker();
        messages = new ArrayList<>();
        Thread.sleep(1000);
        mqttContext = new MqttContext("0.0.0.0", 1883, MqttTbTopic.DEVICE_TELEMETRY_TOPIC);
        mqttBrokerClient = new MqttBrokerClient(mqttContext);
    }

    @Test
    public void publish() throws MqttClientException, InterruptedException{
        Message message = new Message("{\"key1\":\"value1\", \"key2\":\"value2\"}");
        mqttBrokerClient.publish(message);
        Thread.sleep(1000);
        ArrayList<Message> expectedMsg = new ArrayList<>();
        expectedMsg.add(message);

        assertEquals(expectedMsg.get(0).getPayload(), messages.get(0).getPayload());
    }

    @Test
    public void subscribe() throws MqttClientException, InterruptedException{
        String topic = MqttTbTopic.DEVICE_TELEMETRY_TOPIC.toString();
        mqttBrokerClient.subscribe();
        Thread.sleep(1000);

        assertEquals(topic, this.topic);
    }

    @After
    public void tearDown() {
        if (mqttBroker != null) {
            mqttBroker.stopServer();
        }
    }
}
