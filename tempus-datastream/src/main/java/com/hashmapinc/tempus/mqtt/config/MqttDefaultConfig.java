package com.hashmapinc.tempus.mqtt.config;

public class MqttDefaultConfig {
    public static final boolean CLEAN_SESSION = true;

    public static final int CONNECTION_TIMEOUT = 30000;

    public static final boolean AUTOMATIC_RECONNECT = true;

    public static final int MAX_INFLIGHT = 10;

    public static final int KEEP_ALIVE_INTERVAL = 30000;
}
