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
import java.util.Map.Entry;
import java.util.Set;

import java.nio.charset.StandardCharsets;
import java.util.*;


@SparkAction(applicationKey = "SPARK_KAFKA_STREAMING", name = "Spark Kafka Streaming Action", actionClass = "KafkaStreamingSparkComputationAction", descriptor = "KafkaStreamingSparkComputationActionDescriptor.json")
@Configurations(className = "KafkaStreamingSparkComputationConfiguration", mappings = {
        @ConfigurationMapping(field = "zkUrl", type = String.class),
        @ConfigurationMapping(field = "kafkaBrokers", type = String.class),
        @ConfigurationMapping(field = "window", type = Long.class),
        @ConfigurationMapping(field = "topic", type = String.class)
})
@SparkRequest(main = "com.hashmapinc.tempus.spark.WaterLevelAggregator", jar = "spark-kafka-streaming-integration-1.0.0",
        args = {"--topic", "configuration.getTopic()", "--window", "Long.toString(configuration.getWindow())", "--mqttbroker", "configuration.getEndpoint()",
                "--kafka", "configuration.getKafkaBrokers()", "--token", "configuration.getGatewayApiToken()"})
public class WaterLevelAggregator {

    // Access token for 'Analytics Gateway' Device.
    private static String GATEWAY_ACCESS_TOKEN = "aplMzkUg6ziNvfKIOLjL";
    // Kafka brokers URL for Spark Streaming to connect and fetched messages from.
    private static String KAFKA_BROKER_LIST = "kafka:9092";
    // URL of Thingsboard MQTT endpoint
    private static String THINGSBOARD_MQTT_ENDPOINT = "tcp://tb:1883";
    // Time interval in milliseconds of Spark Streaming Job, 10 seconds by default.
    private static int STREAM_WINDOW_MILLISECONDS = 10000; // 10 seconds
    // Kafka telemetry topic to subscribe to. This should match to the topic in the rule action.
    private static Collection<String> TOPICS = Arrays.asList("water-tank-level-data");
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
            SparkConf conf = new SparkConf().setAppName(APP_NAME).setMaster("local[2]");

            try (JavaStreamingContext ssc = new JavaStreamingContext(conf, new Duration(STREAM_WINDOW_MILLISECONDS))) {

            connectToThingsboard();

	    log.info("Topic : " + TOPICS);
	    Set<Entry<String,Object>> hashSet=getKafkaParams().entrySet();
            for(Entry entry:hashSet ) {
 
                log.info("Key="+entry.getKey()+", Value="+entry.getValue());
            }

            JavaInputDStream<ConsumerRecord<String, String>> stream =
                        KafkaUtils.createDirectStream(
                                ssc,
                                LocationStrategies.PreferConsistent(),
                                ConsumerStrategies.<String, String>Subscribe(TOPICS, getKafkaParams())
                        );
	    
	    log.info("Kafka Stream Created...");
            
            stream.foreachRDD(rdd ->
                {
                    // Map incoming JSON to WaterLevelData objects
                    JavaRDD<WaterLevelData> waterLevelRDD = rdd.map(new WeatherStationDataMapper());
                    // Map WaterLevelData objects by TankId
                    JavaPairRDD<String, AvgWaterLevelData> waterLevelByTankRDD = waterLevelRDD.mapToPair(d -> new Tuple2<>(d.getTankId(), new AvgWaterLevelData(d.getWaterTankLevel())));
                    // Reduce all data volume by TankID key
                    JavaPairRDD<String, AvgWaterLevelData> averageWaterLevelByTankRDD = waterLevelByTankRDD.reduceByKey((a, b) -> AvgWaterLevelData.sum(a, b));
                    // Map <TankId, AvgWaterLevelData> back to WaterLevelData
                    List<WaterLevelData> aggData = averageWaterLevelByTankRDD.map(t -> new WaterLevelData(t._1, t._2.getAvgValue())).collect();
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

        private void publishTelemetryToThingsboard(List<WaterLevelData> aggData) throws Exception {
  
	
	if (!aggData.isEmpty()) {
	    
            for (WaterLevelData d : aggData) {
		log.info("*Recvd JSON: "+toConnectJson(d.getTankId()));
                MqttMessage connectMsg = new MqttMessage(toConnectJson(d.getTankId()).getBytes(StandardCharsets.UTF_8));
                client.publish("v1/gateway/connect", connectMsg, null, getCallback());
                
		log.info("*Recvd JSON: "+toDataJson(d));
                MqttMessage dataMsg = new MqttMessage(toDataJson(d).getBytes(StandardCharsets.UTF_8));
                client.publish("v1/gateway/telemetry", dataMsg, null, getCallback());
		}
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

        private static class WeatherStationDataMapper implements Function<ConsumerRecord<String, String>, WaterLevelData> {
            private static final ObjectMapper mapper = new ObjectMapper();

            @Override
            public WaterLevelData call(ConsumerRecord<String, String> record) throws Exception {
                return mapper.readValue(record.value(), WaterLevelData.class);
            }
        }
    }

    private static String toConnectJson(String waterTank) throws JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode json = mapper.createObjectNode();
        json.put("device", "Tank "+waterTank);
        return mapper.writeValueAsString(json);
    }

    private static String toDataJson(WaterLevelData v) throws JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode json = mapper.createObjectNode();
        long ts = System.currentTimeMillis();
      
        ObjectNode tankNode = json.putArray("Tank "+v.getTankId()).addObject();
        tankNode.put("ts", ts);
        ObjectNode values = tankNode.putObject("values");
        values.put("averageWaterLevel", v.getWaterTankLevel());
        
        return mapper.writeValueAsString(json);
    }
}
