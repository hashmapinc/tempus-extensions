# tempus-RPM-and-Torque
Streaming Application to determine the max and min of RPM/Torque and the min and max delta of the drill bit within a given computation window

A usecase - Calculating the delta of min and max of RPM/Torque. Useful in keeping track of drill bit rotational dynamics over time which can be used for decision making regarding the same.

## Table of Contents

- [Requirements](#requirements)
- [Getting Started](#getting-started)
- [Usage](#usage)
- [Known limitations](#limitations)
- [Unit Testing](#unit-testing)
- [ML Using ARIMA Model](#ML)

## Requirements

* Same requirements as specified in TempusDevEnvionment (https://github.com/hashmapinc/TempusDevEnvionment)
* You have followed all the steps specified in TempusDevEnvionment to do all initial setup and testing


## Getting Started
You should find uber jar file in the target subfolder.

## Usage

We need to ensure that Thingsboard and nifi are setup correctly before we build and run this spark program. Go to nifi console and stop all messages.

### Setting up Thingsboard
Assuming you have followed all the steps as specified in TempusDevEnvionment, in your Thingsboard, you should have a Spark Gateway device with GATEWAY_ACCESS_TOKEN credential that will serve as the gateway for this spark program to connect to Thingsboard. You should also have a device called Test Device with myFakeToken credential. You will also have Kafka plugin configured and active. We just need to add a rule specifically to direct nifi messages to rpmdata/torquedata topic. For this do the following:
First go to nifi console and stop nifi messages. This is just to make sure we can make all the changes in both Thingsboard and nifi so we can verify everything is okay before turning it on.
Add a new rule by importing rpm_data_telemetry_rule.json
The rule will be in suspended status. Leave the rule in that status.

### Setting up nifi
Go to nifi console and open configuration for GenerateTimeSeriesFlowFile. Ensure the Timezone is set to "Etc/GMT". This is to make sure that the timestamps for the nifi messages are then in sync with your machine as the timeseries generators seem to use Etc/GMT timing.

Open configuration for GenerateFlowFile configuration. Set Custom Text to {"deviceType":"DrillBit", "deviceId":"123"}

Open a command prompt and change directory to TempusDevEnvionment. Invoke docker ps command and locate nifi container. Then invoke docker exec command to go to spark area of the docker image.

Change directory to /usr/local/configs

Make sure that the basicConfig.json file is either updated for or replaced with the contents specified in drillBit.json.

start the nifi processors now.

### Check timeseries data is coming into Thingsboard
Go to Thingsboard console and click on devices. Then open Test Device. Go to telemetry. And see if the data for drillBit is getting refreshed once every second. Click on Attributes tab and see that deviceType is set to DrillBit and deviceId is set to 123.

### Check Kafka topic rpmdata/torquedata is receiving data
In Thingsboard console, click on rules and activate the "Stick slick RPM rule" rule.

Open a command prompt and change directory to TempusDevEnvionment. Then run docker ps command and locate kafka container. Then invoke docker exec command to get into kafka area of the docker image.

Change directory to /opt/kafka_2.12-0.11.0.0/bin and invoke the following command to check that the messages are comming into the kafka topic:

	./kafka-console-consumer.sh --zookeeper zk:2181 --topic rpmdata

At this point in time, we have ensured all set up is correct and we are ready to build and test out the spark program.

### Build spark code
Use your favorite IDE like Scala IDE or IntelliJ and and import the project in ratechange folder. Build the jar file. The jar file will be named as uber-stickslickrpmandtorque-0.0.1-SNAPSHOT.jar and be available in target folder. Copy this to your SPARK_JAR_DIR. To find out what is the SPARK_JAR_DIR for you, go to TempusDevEnvionment and invoke cat .env|grep SPARK command.

### Test the spark code
Go back to command prompt and change directory to TempusDevEnvionment. Then locate the spark container using docker ps command then then exec the docker command to go to spark area.

	cd /usr/local/apps

Invoke the following command to run the program:

	spark-submit --master local[*] --class com.hashmap.tempus.StickSlickRPMCalculator /usr/local/apps/uber-stickslickrpmandtorque-0.0.1-SNAPSHOT.jar tcp://tb:1883 kafka:9092 rpmdata-data 1 {GATEWAY_ACCESS_TOKEN}
	spark-submit --master local[*] --class com.hashmap.tempus.StickSlickTorqueCalculator /usr/local/apps/uber-stickslickrpmandtorque-0.0.1-SNAPSHOT.jar tcp://tb:1883 kafka:9092 torquedata 1 {GATEWAY_ACCESS_TOKEN}

When you run StickSlickRPMCalculator/StickSlickTorqueCalculator, it looks for messages from the specified topic (e.g. rpmdata); calculates the min and max over the given computation window (1 minute in the example given); calculates the min max delta and then publishes the data to MQTT server with device name as the device ID of the drillBit. The computation result is posted once every given time window.

## Known limitations
Right now the code automatically sets the batchsize to be equal to the windowsize. As a result of which you will see proper output if your window happens to be 1, 2, 3, 4, or 5 minutes long. However, if the window size is changed to anything else you will not get any output.

Apart from that, with Kafka 0.10 and Spark 2.2, if you set batchsize to be less than window size, you will get concurrentModificationException.

## Unit Testing
com.hashmap.unittest.UnitTest shows how unit testing is carried out. To test in Scala IDE, you can build clean and then build test.