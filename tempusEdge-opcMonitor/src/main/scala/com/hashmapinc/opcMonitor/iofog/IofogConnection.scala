package com.hashmapinc.opcMonitor.iofog

import com.iotracks.api.IOFogClient
import com.typesafe.scalalogging.Logger

import com.hashmapinc.opcMonitor.Config

/**
 * This object creates and manages the iofog connection.
 */
object IofogConnection {
  private val log = Logger(getClass())

  val client = new IOFogClient("", 0, Config.CONTAINER_ID) //use default values for client
  val listener = new IofogController()
  
  /**
   * This function connects the web socket logic to the web socket events
   */
  def connect: Unit = {
    //get initial config
    while(!Config.iofogConfig.isDefined) {
      log.info("Requesting config from iofog...")
      client.fetchContainerConfig(listener)
    }

    log.info("Creating iofog connection")
    try {
      client.openMessageWebSocket(listener)
    } catch {
      case e: Exception => log.error("IoFog websocket error: " + e.toString)
    }
    try {
      client.openControlWebSocket(listener)
    } catch {
      case e: Exception => log.error("IoFog websocket error: " + e.toString)
    }
  }
}