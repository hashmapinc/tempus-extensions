package com.hashmapinc.tempus.mqtt.config;

public enum MqttTbTopic {
    DEVICE_ATTRIBUTES_TOPIC("v1/devices/me/attributes"),
    DEVICE_TELEMETRY_TOPIC("v1/devices/me/telemetry"),
    DEVICE_DEPTH_TELEMETRY_TOPIC("v1/devices/me/depth/telemetry"),

    GATEWAY_ATTRIBUTES_TOPIC("v1/gateway/attributes"),
    GATEWAY_TELEMETRY_TOPIC("v1/gateway/telemetry"),
    GATEWAY_DEPTH_TELEMETRY_TOPIC("v1/gateway/depth/telemetry");

    private final String tbTopic;

    private MqttTbTopic(String tbTopic) {
        this.tbTopic = tbTopic;
    }

    public String getTbTopic() {
        return tbTopic;
    }

    @Override
    public String toString() {
        return this.tbTopic;
    }
}
