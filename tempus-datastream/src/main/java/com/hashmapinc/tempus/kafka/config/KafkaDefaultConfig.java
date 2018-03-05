package com.hashmapinc.tempus.kafka.config;

public class KafkaDefaultConfig {
    public static final int AUTO_COMMIT_INTERVAL_MS_CONFIG = 1000;

    public static final boolean ENABLE_AUTO_COMMIT_CONFIG = true;

    public static final int SESSION_TIMEOUT_MS_CONFIG = 30000;

    public static final String KEY_DESERIALIZER_CLASS_CONFIG = "org.apache.kafka.common.serialization.StringDeserializer";

    public static final String KEY_SERIALIZER_CLASS_CONFIG = "org.apache.kafka.common.serialization.StringSerializer";

    public static final String VALUE_DESERIALIZER_CLASS_CONFIG = "org.apache.kafka.common.serialization.StringDeserializer";

    public static final String VALUE_SERIALIZER_CLASS_CONFIG = "org.apache.kafka.common.serialization.StringSerializer";

    public static final int POLLING_INTERVAL_MS_CONFIG = 1000;

    public static final String KAFKA_CLIENT_ID_CONFIG = "kafka-mqtt-client";

    public static final String KAFKA_PRODUCER_ACKS_CONFIG = "all";

    public static final int KAFKA_PRODUCER_RETRIES_CONFIG = 0;
}
