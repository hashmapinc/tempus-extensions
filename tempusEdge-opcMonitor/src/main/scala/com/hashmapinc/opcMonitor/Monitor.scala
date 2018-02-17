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

    //get initial config
    while(!Config.iofogConfig.isDefined) {
      log.info("Requesting config from iofog...")
      IofogConnection.client.fetchContainerConfig(IofogController)
    }
    
    log.info("Connecting to iofog...")
    IofogConnection.connect
    log.info("iofog connection was successful. Listening...")
  }

}
