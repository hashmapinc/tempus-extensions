# tempus-kudu-store
Spark application to store data in Kudu via Impala

## Table of Contents

- [Requirements](#requirements)
- [Getting Started](#getting-started)
- [Usage](#usage)
- [Known limitations](#limitations)
- [Unit Testing](#unit-testing)

## Requirements

## Getting Started

Copy uber-kudu-store-0.0.1-SNAPSHOT.jar file in spark folder.

## Usage

    spark-submit --master local[*] --class com.hashmapinc.tempus.ToKudu uber-kudu-store-0.0.1-SNAPSHOT.jar kafka:9092 well-log-ds-data,well-log-ts-data jdbc:impala://192.168.56.101:21050/kudu_witsml demo demo INFO


## Known limitations

## Unit Testing