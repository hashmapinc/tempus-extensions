package com.hashmapinc.opcMonitor

import com.typesafe.scalalogging.Logger

object Config {
  // general configs
  var context = "production"

  // MQTT configs

  // OPC configs 

  val log = Logger(getClass())
  log.info("Setting default configs")

  /**
   * This function sets the configs to test values
   */
  def initTestContext: Unit = {
    log.info("Setting test configs")
    
    // general configs
    context = "test"

    // MQTT configs

    // OPC configs
  }
}