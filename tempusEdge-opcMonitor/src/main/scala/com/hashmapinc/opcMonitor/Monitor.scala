package com.hashmapinc.opcMonitor

import com.typesafe.scalalogging.Logger

import com.hashmapinc.opcMonitor.iofog.{IofogConnection, IofogController}

/**
 * Driver for the overall OPC Monitoring process
 *
 * @author randypitcherii
 */
object Monitor {
  private val log = Logger(getClass())

  def main(
    args: Array[String]
  ): Unit = {
    log.info("Starting Monitor")
    
    log.info("Connecting to iofog...")
    IofogConnection.connect
    log.info("iofog connection was successful. Listening...")

    while(true) {
      Thread.sleep(5000L)
      println(Config.iofogConfig)
    }
  }
}
