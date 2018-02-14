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
 * This object is responsible for holding and updating opcMonitor configurations
 */
object Config {
  var context = "production"
  var iofogConfig: Option[IofogConfig] = None

  val log = Logger(getClass())
  log.info("Setting default configs")

  /**
   * This function updates the configs
   */
  def update(
    newConfig: IofogConfig): Unit = {
    log.info("Updating config")
    log.debug("newConfig: " + newConfig.toString)

    context = "production"

    //update configs
    iofogConfig = Option(newConfig)
  }

  /**
   * This function sets the configs to test values
   */
  def initTestContext(
    testConfig: IofogConfig): Unit = {
    log.info("Setting test configs")
    log.debug("testConfig: " + testConfig.toString)

    context = "test"

    //update configs
    iofogConfig = Option(testConfig)
  }

  /**
   * This function resets the configs to default values
   */
  def reset: Unit = {
    log.info("Reseting config")

    context = "production"

    //update configs
    iofogConfig = None
  }
}