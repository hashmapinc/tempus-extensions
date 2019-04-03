package com.hashmapinc.tempus

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.regions.RegionUtils
import com.amazonaws.services.kinesis.{AmazonKinesis, AmazonKinesisClient}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kinesis.KinesisInitialPositions.Latest
import org.apache.spark.streaming.kinesis.{KinesisInputDStream, SparkAWSCredentials}
import org.apache.spark.streaming.{Milliseconds, StreamingContext}

import scala.collection.JavaConverters._
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

    val endpoint = options("kinesisEndpoint")
    val region = getRegionNameByEndpoint(endpoint)
    val streamName = options("kinesisStreamName")
    val appName = options("kinesisAppName")

    val kinesisStreams = (0 until getNumberOfShards(endpoint, region, streamName)).map {
      _ =>
        KinesisInputDStream.builder
          .streamingContext(ssc)
          .streamName(streamName)
          .endpointUrl(endpoint)
          .regionName(region)
          .initialPosition(new Latest())
          .checkpointAppName(appName)
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
    List(options.get("kinesisAppName"), options.get("kinesisStreamName"),
      options.get("kinesisEndpoint"), options.get("awsAccessKey"),
      options.get("awsSecretKey")).flatten match {
      case Nil => throw new IllegalArgumentException("Missing all required arguments")
      case l: List[Any] if l.length < 5 => throw new Exception("Required parameters are missing")
      case _ => options
    }
  }

  private def getRegionNameByEndpoint(endpoint: String): String = {
    val uri = new java.net.URI(endpoint)
    RegionUtils.getRegionsForService(AmazonKinesis.ENDPOINT_PREFIX)
      .asScala
      .find(_.getAvailableEndpoints.asScala.toSeq.contains(uri.getHost))
      .map(_.getName)
      .getOrElse(
        throw new IllegalArgumentException(s"Could not resolve region for endpoint: $endpoint"))
  }
}
