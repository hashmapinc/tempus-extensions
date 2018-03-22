package com.hashmapinc.tempus.mqtt.exception;

import org.eclipse.paho.client.mqttv3.MqttException;

public class MqttClientException extends MqttException {
    public MqttClientException(Exception exception) {
        super(exception);
    }
}
