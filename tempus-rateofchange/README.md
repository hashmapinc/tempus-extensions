# tempus-rateofchange
Streaming Application to determine the rate of change of a curve and estimating the time to a limit

A usecase where this is expected to be applicable is when a pump starts pumping out gas, it also pumps out water to a water tank. As the water tank fills up, it needs to emptied up or else the pump will have to be shut down and as long as the pump is shutdown, no revenue for the oil & gas company.

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
Open a command line and go to a dirctory where you will want to this project to be and clone as under:

	git clone https://github.com/hashmapinc/tempus-rateofchange.git

## Usage

We need to ensure that Thingsboard and nifi are setup correctly before we build and run this spark program. Go to nifi console and stop all messages.

### Setting up Thingsboard
Assuming you have followed all the steps as specified in TempusDevEnvionment, in your Thingsboard, you should have a Spark Gateway device with GATEWAY_ACCESS_TOKEN credential that will serve as the gateway for this spark program to connect to Thingsboard. You should also have a device called Test Device with myFakeToken credential. You will also have Kafka plugin configured and active. We just need to add a rule specifically to direct nifi messages to water-tank-level-data topic. For this do the following:
First go to nifi console and stop nifi messages. This is just to make sure we can make all the changes in both Thingsboard and nifi so we can verify everything is okay before turning it on.
Add a new rule by importing watertank_level_telemetry_rule.json
The rule will be in suspended status. Leave the rule in that status.

### Setting up nifi
Go to nifi console and open configuration for GenerateTimeSeriesFlowFile. Ensure the Timezone is set to "Etc/GMT". This is to make sure that the timestamps for the nifi messages are then in sync with your machine as the timeseries generators seem to use Etc/GMT timing.

Open configuration for GenerateFlowFile configuration. Set Custom Text to {"deviceType":"WaterTank", "tankId":"123"}

Open a command prompt and change directory to TempusDevEnvionment. Invoke docker ps command and locate spark container. Then invoke docker exec command to go to spark area of the docker image.

Change directory to /usr/local/configs

Make sure that the basicConfig.json file is either updated for or replaced with the contents specified in tankVolumeConfig.json.

start the nifi processors now.

### Check timeseries data is coming into Thingsboard
Go to Thingsboard console and click on devices. Then open Test Device. Go to telemetry. And see if the data for waterTankLevel is getting refreshed once every second. Click on Attributes tab and see that deviceType is set to WaterTank and tankId is set to 123.

### Check Kafka topic water-tank-level-data is receiving data
In Thingsboard console, click on rules and activate the "Watertank Level Telemetry Rule" rule.

Open a command prompt and change directory to TempusDevEnvionment. Then run docker ps command and locate kafka container. Then invoke docker exec command to get into kafka area of the docker image.

Change directory to /opt/kafka_2.12-0.11.0.0/bin and invoke the following command to check that the messages are comming into the kafka topic:

	./kafka-console-consumer.sh --zookeeper zk:2181 --topic water-tank-level-data

At this point in time, we have ensured all set up is correct and we are ready to build and test out the spark program.

### Build spark code
Use your favorite IDE like Scala IDE or IntelliJ and and import the project in ratechange folder. Build the jar file. The jar file will be named as uber-ratechange-0.0.1-SNAPSHOT.jar and be available in ratechange/target folder. Copy this to your SPARK_JAR_DIR. To find out what is the SPARK_JAR_DIR for you, go to TempusDevEnvionment and invoke cat .env|grep SPARK command. 

### Test the spark code
Go back to command prompt and change directory to TempusDevEnvionment. Then locate the spark container using docker ps command then then exec the docker command to go to spark area.

	cd /usr/local/apps

If you want to get debug messages while running the program, invoke the following command:

	spark-submit --master local[*] --class com.hashmap.tempus.WaterLevelPredictor /usr/local/apps/uber-ratechange-0.0.1-SNAPSHOT.jar tcp://tb:1883 kafka:9092 water-tank-level-data 70.0 1 DEBUG=INFO

If you do not want debug messages while running the program, invoke the following command:

	spark-submit --master local[*] --class com.hashmap.tempus.WaterLevelPredictor /usr/local/apps/uber-ratechange-0.0.1-SNAPSHOT.jar tcp://tb:1883 kafka:9092 water-tank-level-data 70.0 1

When you run WaterLevelPredictor, it looks for messages from the specified topic (e.g. water-tank-level-data); shuffles messages for each tank (e.g. 123, 124 etc.); and publishes the result of ETA to fill the water tank to the high mark (e.g. 70.0). The messages are published once every time slice (e.g. 1 minute). It creates "Tank 123", "Tank 124" devices etc. in thingsboard if they don't exist.

## Known limitations
Right now the code automatically sets the batchsize to be equal to the windowsize. As a result of which you will see proper output if your window happens to be 1, 2, 3, 4, or 5 minutes long. However, if the window size is changed to anything else you will not get any output.

Apart from that, with Kafka 0.10 and Spark 2.2, if you set batchsize to be less than window size, you will get concurrentModificationException.

## Unit Testing
com.hashmap.unittest.UnitTest shows how unit testing is carried out. To test in Scala IDE, you can build clean and then build test. 

## ML Using ARIMA Model
The same solution is given using ARIMA model. In actuality, the ML implementation will be broken into two parts - a) modeling, and 2) forecasting. In the modeling part a good amount of data will be used for each water tank and a model will be determined. Once a model is determined, the model parameters such as p, d, and q will be stored in the DB along with the tank ID. This configuration will be picked up by the forecaster to set a model and start forecasting using a streaming data.

For now, both modeling and forecasting is put into one file. The current program will process only one tank at a time. The tank ID has to be provided as a parameter . The command line looks as follows:


	spark-submit --master local[*] --class com.hashmap.tempus.WaterLevelPredictorAM /usr/local/apps/uber-ratechange-0.0.1-SNAPSHOT.jar tcp://tb:1883 kafka:9092 water-tank-level-data 123 70.0 1


