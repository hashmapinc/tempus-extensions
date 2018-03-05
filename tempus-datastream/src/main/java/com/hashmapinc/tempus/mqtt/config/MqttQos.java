package com.hashmapinc.tempus.mqtt.config;

public enum MqttQos {

    QOS0(0),
    QOS1(1);

    private final int qos;

    private MqttQos(final int qos) {
        this.qos = qos;
    }

    public int getValue() {
        return this.qos;
    }

    public static MqttQos valueOf(int qos) {
        if (qos == 0) {
            return QOS0;
        } else if (qos == 1) {
            return QOS1;
        } else {
            throw new IllegalArgumentException("QoS not supported");
        }
    }

}
