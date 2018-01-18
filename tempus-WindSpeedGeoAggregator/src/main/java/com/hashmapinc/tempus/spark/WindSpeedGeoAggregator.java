package com.hashmapinc.tempus.spark;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.hashmap.tempus.annotations.ConfigurationMapping;
import com.hashmap.tempus.annotations.Configurations;
import com.hashmap.tempus.annotations.SparkAction;
import com.hashmap.tempus.annotations.SparkRequest;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.cli.*;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.eclipse.paho.client.mqttv3.IMqttActionListener;
import org.eclipse.paho.client.mqttv3.IMqttToken;
import org.eclipse.paho.client.mqttv3.MqttAsyncClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import scala.Tuple2;

import java.nio.charset.StandardCharsets;
import java.util.*;


@SparkAction(applicationKey = "SPARK_KAFKA_STREAMING", name = "Spark Kafka Streaming Action", actionClass = "KafkaStreamingSparkComputationAction", descriptor = "KafkaStreamingSparkComputationActionDescriptor.json")
@Configurations(className = "KafkaStreamingSparkComputationConfiguration", mappings = {
        @ConfigurationMapping(field = "zkUrl", type = String.class),
        @ConfigurationMapping(field = "kafkaBrokers", type = String.class),
        @ConfigurationMapping(field = "window", type = Long.class),
        @ConfigurationMapping(field = "topic", type = String.class)
})
@SparkRequest(main = "com.hashmapinc.tempus.spark.WindSpeedGeoAggregator", jar = "spark-kafka-streaming-integration-1.0.0",
        args = {"--topic", "configuration.getTopic()", "--window", "Long.toString(configuration.getWindow())", "--mqttbroker", "configuration.getEndpoint()",
                "--kafka", "configuration.getKafkaBrokers()", "--token", "configuration.getGatewayApiToken()"})
public class WindSpeedGeoAggregator {

    // Access token for 'Analytics Gateway' Device.
    private static String GATEWAY_ACCESS_TOKEN = "aplMzkUg6ziNvfKIOLjL";
    // Kafka brokers URL for Spark Streaming to connect and fetched messages from.
    private static String KAFKA_BROKER_LIST = "kafka:9092";
    // URL of Thingsboard MQTT endpoint
    private static String THINGSBOARD_MQTT_ENDPOINT = "tcp://tb:1883";
    // Time interval in milliseconds of Spark Streaming Job, 10 seconds by default.
    private static int STREAM_WINDOW_MILLISECONDS = 10000; // 10 seconds
    // Kafka telemetry topic to subscribe to. This should match to the topic in the rule action.
    private static Collection<String> TOPICS = Arrays.asList("weather-stations-data");
    // The application name
    public static final String APP_NAME = "Kafka Spark Streaming App";


    public static void main(String[] args) throws Exception {
        Options options = new Options();
        options.addOption("token", true, "MQTT Access Token");
        options.addOption("kafka", true, "Kafka Broker List");
        options.addOption("mqttbroker", true, "MQTT endpoint");
        options.addOption("window", true, "Stream window (milliseconds)");
        options.addOption("topic", true, "Kafka topic to pull data from");

        CommandLineParser parser = new BasicParser();
        CommandLine parameters = parser.parse(options, args);

        String topic = parameters.getOptionValue("topic");
        if (topic == null) {
            PrintHelp(options);
            return;
        }
        System.out.println(topic);
        TOPICS = Collections.singletonList(topic);
        String window = parameters.getOptionValue("window");
        if (window == null) {
            PrintHelp(options);
            return;
        }
        int windowVal = -1;
        try {
            windowVal = Integer.parseInt(window);
        }catch (NumberFormatException ex){
            PrintHelp(options);
            return;
        }
        System.out.println(windowVal);
        STREAM_WINDOW_MILLISECONDS = windowVal;

        String mqttbroker = parameters.getOptionValue("mqttbroker");
        if (mqttbroker == null) {
            System.out.println("broker is null");
            PrintHelp(options);
            return;
        }
        System.out.println(mqttbroker);
        THINGSBOARD_MQTT_ENDPOINT = mqttbroker;

        String kafka = parameters.getOptionValue("kafka");
        if (kafka == null) {
            PrintHelp(options);
            return;
        }
        System.out.println(kafka);
        KAFKA_BROKER_LIST = kafka;

        String token = parameters.getOptionValue("token");
        if (token == null) {
            PrintHelp(options);
            return;
        }
        System.out.println(token);
        GATEWAY_ACCESS_TOKEN = token;

        new StreamRunner().start();
    }

    private static void PrintHelp(Options options){
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("Kafka Spark Streaming App", options);
    }

    // Misc Kafka client properties
    private static Map<String, Object> getKafkaParams() {
        Map<String, Object> kafkaParams = new
                HashMap<>();
        kafkaParams.put("bootstrap.servers", KAFKA_BROKER_LIST);
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "DEFAULT_GROUP_ID");
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);
        return kafkaParams;
    }

    @Slf4j
    private static class StreamRunner {

        private final MqttAsyncClient client;

        StreamRunner() throws MqttException {
            client = new MqttAsyncClient(THINGSBOARD_MQTT_ENDPOINT, MqttAsyncClient.generateClientId());
        }

        void start() throws Exception {
            SparkConf conf = new SparkConf().setAppName(APP_NAME).setMaster("local");

            try (JavaStreamingContext ssc = new JavaStreamingContext(conf, new Duration(STREAM_WINDOW_MILLISECONDS))) {

                connectToThingsboard();

                JavaInputDStream<ConsumerRecord<String, String>> stream =
                        KafkaUtils.createDirectStream(
                                ssc,
                                LocationStrategies.PreferConsistent(),
                                ConsumerStrategies.<String, String>Subscribe(TOPICS, getKafkaParams())
                        );

                stream.foreachRDD(rdd ->
                {
                    // Map incoming JSON to WindSpeedData objects
                    JavaRDD<WindSpeedData> windRdd = rdd.map(new WeatherStationDataMapper());
                    // Map WindSpeedData objects by GeoZone
                    JavaPairRDD<String, AvgWindSpeedData> windByZoneRdd = windRdd.mapToPair(d -> new Tuple2<>(d.getGeoZone(), new AvgWindSpeedData(d.getWindSpeed())));
                    // Reduce all data volume by GeoZone key
                    windByZoneRdd = windByZoneRdd.reduceByKey((a, b) -> AvgWindSpeedData.sum(a, b));
                    // Map <GeoZone, AvgWindSpeedData> back to WindSpeedData
                    List<WindSpeedData> aggData = windByZoneRdd.map(t -> new WindSpeedData(t._1, t._2.getAvgValue())).collect();
                    // Push aggregated data to Thingsboard using Gateway MQTT API
                    publishTelemetryToThingsboard(aggData);
                });

                ssc.start();
                ssc.awaitTermination();
            }
        }

        private void connectToThingsboard() throws Exception {
            MqttConnectOptions options = new MqttConnectOptions();
            options.setUserName(GATEWAY_ACCESS_TOKEN);
            try {
                client.connect(options, null, new IMqttActionListener() {
                    @Override
                    public void onSuccess(IMqttToken iMqttToken) {
                        log.info("Connected to Thingsboard!");
                    }

                    @Override
                    public void onFailure(IMqttToken iMqttToken, Throwable e) {
                        log.error("Failed to connect to Thingsboard!", e);
                    }
                }).waitForCompletion();
            } catch (MqttException e) {
                log.error("Failed to connect to the server", e);
            }
        }

        private void publishTelemetryToThingsboard(List<WindSpeedData> aggData) throws Exception {
            if (!aggData.isEmpty()) {
                for (WindSpeedData d : aggData) {
                    MqttMessage connectMsg = new MqttMessage(toConnectJson(d.getGeoZone()).getBytes(StandardCharsets.UTF_8));
                    client.publish("v1/gateway/connect", connectMsg, null, getCallback());
                }
                MqttMessage dataMsg = new MqttMessage(toDataJson(aggData).getBytes(StandardCharsets.UTF_8));
                client.publish("v1/gateway/telemetry", dataMsg, null, getCallback());
            }
        }

        private IMqttActionListener getCallback() {
            return new IMqttActionListener() {
                @Override
                public void onSuccess(IMqttToken asyncActionToken) {
                    log.info("Telemetry data updated!");
                }

                @Override
                public void onFailure(IMqttToken asyncActionToken, Throwable exception) {
                    log.error("Telemetry data update failed!", exception);
                }
            };
        }

        private static class WeatherStationDataMapper implements Function<ConsumerRecord<String, String>, WindSpeedData> {
            private static final ObjectMapper mapper = new ObjectMapper();

            @Override
            public WindSpeedData call(ConsumerRecord<String, String> record) throws Exception {
                return mapper.readValue(record.value(), WindSpeedData.class);
            }
        }
    }

    private static String toConnectJson(String geoZone) throws JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode json = mapper.createObjectNode();
        json.put("device", geoZone);
        return mapper.writeValueAsString(json);
    }

    private static String toDataJson(List<WindSpeedData> aggData) throws JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode json = mapper.createObjectNode();
        long ts = System.currentTimeMillis();
        aggData.forEach(v -> {
            ObjectNode zoneNode = json.putArray(v.getGeoZone()).addObject();
            zoneNode.put("ts", ts);
            ObjectNode values = zoneNode.putObject("values");
            values.put("windSpeed", v.getWindSpeed());
        });
        return mapper.writeValueAsString(json);
    }
}