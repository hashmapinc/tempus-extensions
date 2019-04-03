package com.hashmapinc.tempus

import java.lang
import java.util.Optional

import com.google.gson.{Gson, GsonBuilder}
import com.hashmap.tempus.annotations.SparkRequest
import com.hashmap.tempus.models.ArgType
import org.apache.log4j.LogManager
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream

import scala.math.abs


@SparkRequest(main = "com.hashmapinc.tempus.RigStateCalculationJob", jar = "uber-tempus-rig-state-0.0.1-SNAPSHOT.jar",
  name = "Rig State", descriptor = "RigStateDescriptor.json",
  args = Array("mqttUrl", "gatewayAccessToken"), argType = ArgType.NAMED)
class RigStateCalculationJob(val options: OptionsMap) extends Job{

  import RigStateCalculationJob._

  override def process(ssc: StreamingContext): DStream[String] => Unit = { ds =>
    val checkPointDir = System.getenv().getOrDefault("CHECKPOINT_DIR", ".")
    ssc.checkpoint(checkPointDir)

    calculateRigState(ds, options)
  }
}

object RigStateCalculationJob{

  private val log = LogManager.getLogger(this.getClass)

  def calculateRigState(ds: DStream[String], options: OptionsMap): Unit ={
    log.info("Starting rig state data calculation for option " + options)
    ds.filter(!_.trim.isEmpty)
      .map(parseAndPairByKey)
      .updateStateByKey[RigStateData](processAndUpdateState(options) _)
      .print()
  }

  def processAndUpdateState(options: OptionsMap)(newRSD: Seq[RigStateData], prevRSD: Option[RigStateData]): Option[RigStateData] = {
    if(newRSD.nonEmpty) newRSD.foldLeft(prevRSD)(processRecord(options)) else prevRSD
  }

  def processRecord(options: OptionsMap)(prevRSD: Option[RigStateData], currRSD: RigStateData): Option[RigStateData] = {
    if (prevRSD.isEmpty) {
      Some(currRSD)
    } else {

      val rigState = calculateRigState(prevRSD.get, currRSD)
      val data = PublishRigStateData(rigState)

      publishToTempusCloud(currRSD, data, options)

      Some(currRSD)
    }
  }

  def publishToTempusCloud(currRSD: RigStateData, data: PublishRigStateData, options: OptionsMap): Unit = {
    val json = new GsonBuilder().create.toJson(data)
    val empty: Optional[lang.Double] = Optional.ofNullable(null)
    (options.get("mqttUrl"), options.get("gatewayAccessToken")) match {
      case (Some(m: String), Some(g: String)) =>
        new MqttConnector(m, g).publish(json, Optional.of(currRSD.ts), empty, currRSD.id)
        log.info(s"Published data to mqtt server: $m.value with payload $data ")
      case _ => throw new IllegalArgumentException("Missing MqttUrl and Gateway access token")
    }
  }

  def calculateRigState(prevRSD: RigStateData, currRSD: RigStateData): String = {
    if (currRSD.bitDepth == -999.25 && currRSD.holeDepth == -999.25) "Depth Data Issue"
    else if (currRSD.bitDepth <= 0 && currRSD.bitDepth != -999.25 && currRSD.holeDepth > 0) "Out of Hole"
    else {
      val pipeDirection = calculatePipeDirection(prevRSD, currRSD)
      val pumpStatus = calculatePumpStatus(currRSD)
      val topDriveStatus = calculateTopDriveStatus(currRSD)
      val bitOnBottom = calculateBitOnBottom(currRSD)

      if (pipeDirection == 0 && pumpStatus == -1 && topDriveStatus == -1 && currRSD.inSlipStatus == 0 && bitOnBottom == 1) "Stationary"
      else if (pipeDirection == 0 && pumpStatus == -1 && topDriveStatus == 1 && currRSD.inSlipStatus == 0 && bitOnBottom == 1) "Rotating"
      else if (pipeDirection == 0 && pumpStatus == 1 && topDriveStatus == 1 && currRSD.inSlipStatus == 0 && bitOnBottom == 1) "Rotating Pumping"
      else if (pipeDirection == 0 && pumpStatus == 1 && topDriveStatus == -1 && bitOnBottom == 1) "Pumping"
      else if (pipeDirection == -1 && pumpStatus == 1 && topDriveStatus == 1 && currRSD.inSlipStatus == 0 && bitOnBottom == 0) "Rotary Drilling"
      else if (pipeDirection == -1 && pumpStatus == 1 && topDriveStatus == -1 && currRSD.inSlipStatus == 0 && bitOnBottom == 0) "Slide Drilling"
      else if (pipeDirection == 1 && pumpStatus == 1 && topDriveStatus == -1 && currRSD.inSlipStatus == 0 && bitOnBottom == 1) "Tripping Out Pumping"
      else if (pipeDirection == 1 && pumpStatus == -1 && topDriveStatus == -1 && currRSD.inSlipStatus == 0 && bitOnBottom == 1) "Tripping Out"
      else if (pipeDirection == 1 && pumpStatus == -1 && topDriveStatus == 1 && currRSD.inSlipStatus == 0 && bitOnBottom == 1) "Tripping Out Rotating"
      else if (pipeDirection == -1 && pumpStatus == 1 && topDriveStatus == -1 && currRSD.inSlipStatus == 0 && bitOnBottom == 1) "Tripping In Pumping"
      else if (pipeDirection == -1 && pumpStatus == -1 && topDriveStatus == -1 && currRSD.inSlipStatus == 0 && bitOnBottom == 1) "Tripping In"
      else if (pipeDirection == -1 && pumpStatus == -1 && topDriveStatus == 1 && currRSD.inSlipStatus == 0 && bitOnBottom == 1) "Tripping In Rotating"
      else if (currRSD.inSlipStatus == 1 && bitOnBottom == 1) "In Slips"
      else if (pipeDirection == 1 && pumpStatus == 1 && topDriveStatus == 1 && currRSD.inSlipStatus == 0 && bitOnBottom == 1) "Back Ream"
      else if (pipeDirection == -1 && pumpStatus == 1 && topDriveStatus == 1 && currRSD.inSlipStatus == 0 && bitOnBottom == 1) "Ream In"
      else "Unknown"
    }
  }

  private def calculatePipeDirection(previousRigStateData: RigStateData, currentRigStateData: RigStateData) = {
    if (abs(currentRigStateData.bitDepth - previousRigStateData.bitDepth) <= 0.1) 0
    else if (currentRigStateData.bitDepth - previousRigStateData.bitDepth > 0) -1
    else if (currentRigStateData.bitDepth - previousRigStateData.bitDepth < 0) 1
    else Integer.MAX_VALUE
  }

  private def calculatePumpStatus(currentRigStateData: RigStateData) = {
    if (currentRigStateData.totalPumpOutput <= 0.00630902) -1
    else if (currentRigStateData.totalPumpOutput > 0.00630902 && currentRigStateData.standpipePressure >= 3447380) 1
    else Integer.MAX_VALUE
  }

  private def calculateTopDriveStatus(currentRigStateData: RigStateData) = {
    if (currentRigStateData.rotaryRpm <= 1.05) -1
    else if (currentRigStateData.rotaryRpm > 3.15) 1
    else if (currentRigStateData.rotaryRpm > 1.05 && currentRigStateData.rotaryRpm  <= 3.15) 0
    else Integer.MAX_VALUE
  }

  private def calculateBitOnBottom(currentRigStateData: RigStateData) = {
    if (currentRigStateData.holeDepth - currentRigStateData.bitDepth < (-0.3048)) -1
    else if (currentRigStateData.holeDepth - currentRigStateData.bitDepth < 0.33) 0
    else 1
  }

  def parseAndPairByKey(jsonStr: String): (String, RigStateData) = {
    val data: RigStateData = new Gson().fromJson(jsonStr.trim, classOf[RigStateData])
    (data.id, data)
  }
}
