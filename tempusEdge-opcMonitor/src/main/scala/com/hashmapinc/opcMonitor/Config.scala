package com.hashmapinc.opcMonitor

object Config {
  // general configs
  var context = "production"

  // MQTT configs

  // OPC configs 

  /**
   * This function sets the configs to test values
   */
  def initTestContext: Unit = {
    // general configs
    context = "test"

    // MQTT configs

    // OPC configs
  }
}