package com.hashmapinc.opcMonitor

import com.typesafe.scalalogging.Logger

import com.hashmapinc.opcMonitor.iofog.IofogConnection
import com.hashmapinc.opcMonitor.mqtt.MqttController
import com.hashmapinc.opcMonitor.opc.OpcController

/**
 * Driver for the overall OPC Monitoring process
 *
 * @author randypitcherii
 */
object Monitor {
  val log = Logger(getClass())

  def main(
    args: Array[String]
  ): Unit = {
    log.info("Starting Monitor")
    IofogConnection.connect
  }

}
