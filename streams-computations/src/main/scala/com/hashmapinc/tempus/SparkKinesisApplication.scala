package com.hashmapinc.tempus

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.regions.RegionUtils
import com.amazonaws.services.kinesis.{AmazonKinesis, AmazonKinesisClient}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kinesis.KinesisInitialPositions.Latest
import org.apache.spark.streaming.kinesis.{KinesisInputDStream, SparkAWSCredentials}
import org.apache.spark.streaming.{Milliseconds, StreamingContext}

import scala.util.{Failure, Success, Try}

object SparkKinesisApplication extends StreamingContextHelper {

  def run(options: OptionsMap, j: OptionsMap => Try[Job]): Unit = {
    withContext(options){ ssc =>
      j(options) match {
        case Success(job) =>
          val stream = buildKinesisStream(options, ssc)
          job.withStreamingContext(ssc)(stream.map(new String(_)))
        case Failure(e) => throw e
      }
    }
  }

  private def buildKinesisStream(options: OptionsMap, ssc: StreamingContext) = {
    validateRequiredArguments(options)

    val region = options("kinesisRegion")
    val endpoint = getEndpoint(region)
    val streamName = options("kinesisStreamName")

    val kinesisStreams = (0 until getNumberOfShards(endpoint, region, streamName)).map {
      _ =>
        KinesisInputDStream.builder
          .streamingContext(ssc)
          .streamName(streamName)
          .endpointUrl(endpoint)
          .regionName(region)
          .initialPosition(new Latest())
          .checkpointAppName(streamName)
          .checkpointInterval(Milliseconds(5000))
          .storageLevel(StorageLevel.MEMORY_ONLY)
          .build()
    }
    ssc.union(kinesisStreams)
  }

  private def getNumberOfShards(endpoint: String, region: String, streamName: String) = {
    AmazonKinesisClient.builder
      .withCredentials(new DefaultAWSCredentialsProviderChain())
      .withEndpointConfiguration(new EndpointConfiguration(endpoint, region))
      .build
      .describeStream(streamName)
      .getStreamDescription
      .getShards
      .size
  }

  private def awsCredentials(accessKeyId: String, secretKey: String) ={
    SparkAWSCredentials
      .builder
      .basicCredentials(accessKeyId, secretKey)
      .build()
  }

  private def validateRequiredArguments(options: OptionsMap): OptionsMap ={
    List(options.get("kinesisStreamName"),
      options.get("kinesisRegion"), options.get("awsAccessKey"),
      options.get("awsSecretKey")).flatten match {
      case Nil => throw new IllegalArgumentException("Missing all required arguments")
      case l: List[Any] if l.length < 4 => throw new Exception("Required parameters are missing")
      case _ => options
    }
  }

  private def getEndpoint(region: String): String = {
    RegionUtils.getRegion(region).getServiceEndpoint(AmazonKinesis.ENDPOINT_PREFIX)
  }
}
