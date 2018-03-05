package com.hashmapinc.tempus.mqtt.config;

public enum MqttProtocolType {
    TCP ("tcp://"),
    SSL ("ssl://");

    private final String connectionType;

    private MqttProtocolType(String type) {
        connectionType = type;
    }

    public String getConnectionType() {
        return connectionType;
    }

    @Override
    public String toString() {
        return this.connectionType;
    }
}
