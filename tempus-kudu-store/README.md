# tempus-kudu-store
In tempus environment, data in KUDU can be captured via a Spark application with Impala interface. The data from NIFI
first flows into Thingsboard. With rules configured in Thingsboard, the telemetry and attributes data is published to
specific Kafka topics. Spark program reads data off of Kafka topics and pushes data into KUDU store.

## Table of Contents

- [Requirements](#requirements)
- [Getting Started](#getting-started)
- [Usage](#usage)
- [Known limitations](#limitations)
- [Unit Testing](#unit-testing)

## Requirements

## Getting Started

Copy uber-kudu-store-0.0.1-SNAPSHOT.jar file in spark folder.
Make sure you have necessary KUDU tables created. Use dbscript file for more details.

## Usage

    spark-submit --master local[*] --class com.hashmapinc.tempus.ToKudu uber-kudu-store-0.0.1-SNAPSHOT.jar kafka:9092 well-log-ds-data,well-log-ts-data jdbc:impala://192.168.56.101:21050/kudu_witsml demo demo INFO
    spark-submit --master local[*] --class com.hashmapinc.tempus.AttributesToKudu uber-kudu-store-0.0.1-SNAPSHOT.jar kafka:9092 well-attribute-data jdbc:impala://192.168.56.101:21050/kudu_witsml demo demo INFO


## Known limitations

## Unit Testing