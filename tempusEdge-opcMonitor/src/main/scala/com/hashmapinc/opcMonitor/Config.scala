package com.hashmapinc.opcMonitor

import com.typesafe.scalalogging.Logger

/**
 *  Case class for structuring incoming iofog JSON configurations.
 */
case class IofogConfig(
  // OPC configs 
  opcHost: String,
  opcPort: Int,

  // MQTT configs
  mqttHost: String,
  mqttPort: Int,
  mqttTopic: String,

  authToken: String)

/**
 * This object is responsible
 */
object Config {
  var context = "production"
  var iofogConfig: Option[IofogConfig] = None

  val log = Logger(getClass())
  log.info("Setting default configs")

  /**
   * This function sets the configs to test values
   */
  def initTestContext(
    testConfig: IofogConfig): Unit = {
    log.info("Setting test configs")

    // general configs
    context = "test"

    //update configs
    iofogConfig = Option(testConfig)
  }
}