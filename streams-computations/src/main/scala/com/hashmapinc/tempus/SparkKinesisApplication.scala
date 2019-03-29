package com.hashmapinc.tempus

import java.nio.charset.StandardCharsets

import com.amazonaws.regions.RegionUtils
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kinesis.{KinesisInitialPositions, KinesisInputDStream, SparkAWSCredentials}
import org.apache.spark.streaming.{Milliseconds, StreamingContext}

import scala.util.{Failure, Success, Try}

object SparkKinesisApplication extends StreamingContextHelper {

  def run(options: OptionsMap, j: OptionsMap => Try[Job]): Unit = {
    withContext(options){ ssc =>
      j(options) match {
        case Success(job) =>
          val stream = buildKinesisStream(options, ssc)
          job.withStreamingContext(ssc)(stream.map(b => new String(b, StandardCharsets.UTF_8)))
        case Failure(e) => throw e
      }
    }
  }

  private def buildKinesisStream(options: OptionsMap, ssc: StreamingContext) = {
    validateRequiredArguments(options)
    KinesisInputDStream.builder
      .streamName(options("kinesisStreamName"))
      .checkpointAppName(options("kinesisAppName"))
      .endpointUrl(options("kinesisEndpoint"))
      .regionName(RegionUtils.getRegionMetadata
        .getRegion(options("kinesisRegion")).getName)
      .kinesisCredentials(
        awsCredentials(options("awsAccessKey"), options("awsSecretKey")))
      .checkpointInterval(Milliseconds(5000))
      .initialPosition(KinesisInitialPositions.fromKinesisInitialPosition(InitialPositionInStream.LATEST))
      .storageLevel(StorageLevel.MEMORY_ONLY)
      .streamingContext(ssc)
      .build()
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
      options.get("awsSecretKey"), options.get("kinesisRegion")).flatten match {
      case Nil => throw new IllegalArgumentException("Missing all required arguments")
      case l: List[Any] if l.length < 6 => throw new Exception("Required parameters are missing")
      case _ => options
    }

  }

}
