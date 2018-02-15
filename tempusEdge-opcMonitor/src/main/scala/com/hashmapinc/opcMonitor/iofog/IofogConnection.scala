package com.hashmapinc.opcMonitor.iofog

import com.iotracks.api.IOFogClient
import com.typesafe.scalalogging.Logger

import com.hashmapinc.opcMonitor.Config

/**
 * This object creates and manages the iofog connection.
 */
object IofogConnection {
  val log = Logger(getClass())

  val client = new IOFogClient("", 0, Config.CONTAINER_ID) //use default values for client
  
  /**
   * This function connects the web socket logic to the web socket events
   */
  def connect: Unit = {
    log.info("Creating iofog connection")
    try {
      client.openControlWebSocket(IofogController);
    } catch {
      case e: Exception => log.error("IoFog websocket error: " + e.toString)
    }

    try {
      client.openMessageWebSocket(IofogController);
    } catch {
      case e: Exception => log.error("IoFog websocket error: " + e.toString)
    }
  }
}