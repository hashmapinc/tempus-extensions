# Tempus Data Streams

Tempus Data Stream is library used to create stream. Presently it provide Kafka-Mqtt Stream api's to create stream from Kafka-Consumer to Mqtt-Publisher by providing minimal properties.

## Table of Contents

- [Features](#features)
- [Requirements](#requirements)
- [API Usage](#api-usage)

## Features
This library provide direct api's to make a stream from Kafka Consumer to publish data to Mqtt Thingsboard Broker.
By providing minimal configuration properties for Kafka Consumer and Mqtt Broker Client, it will create continuous stream for polling kafka topic and publish it to thingsboard mqtt broker.

## Requirements
If SSL based authentication is enabled on Thingsboard to connect MqttBroker, don't forget to provide Keystore and Trust Keystore JKS files.
And also provide Password to Keystore and Trust Keystore.
```java
MqttSslContext(String pathKeyStoreFile, String keyPassword, String keyStorePassword, String pathTrustStoreFile, String trustStorePassword);
```

## API Usage
To create Kafka to Mqtt Stream create object of `KafkaToMqttStream` class and call the function `createKafkaToMqttStream`.
This function require  `KafkaConsumerContext` and `MqttContext` which have Ip, Port, Topic and other Properties for both Kafka and Mqtt broker.

##### Create KafkaConsumerContext
```java
KafkaConsumerContext(String kafkaServerIp, int kafkaServerPort, String kafkaTopic);
                                                        OR
KafkaConsumerContext(String kafkaServerIp, int kafkaServerPort, String kafkaTopic, Properties properties);
```
If properties are not provided default properties will be applied to KafmaConsumer.

##### Create MqttContext
```java
MqttContext(String mqttBrokerIp, int mqttBrokerPort, MqttTbTopic topic, String username)  /**TCP**/
                                                        OR
MqttContext(String mqttBrokerIp, int mqttBrokerPort, MqttTbTopic topic, MqttSslContext mqttSslContext) throws MqttSslException   /**SSL**/
```
Select the MqttConext based on the authentication supported by the Thingsboard. We can have TCP or SSL based authentication.
 
##### Create KafkaToMqttStream
```java
KafkaToMqttStream kafkaToMqttStream = new KafkaToMqttStream();


try {
    kafkaToMqttStream.createKafkaToMqttStream(kafkaConsumerContext, mqttContext);
} catch (Exception ex) {
    ex.printStackTrace();
}
```
This will start the Kafka Consumer which will continously poll the topic and publish the message received to Thingsboard Mqtt Broker.

