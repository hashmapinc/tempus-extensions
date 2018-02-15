package com.hashmapinc.opcMonitor

import com.typesafe.scalalogging.Logger
import play.api.libs.json.Json

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

  authToken: String
)
object IofogConfig {
  // define implicit config reader for json to case-class conversion
  // https://www.playframework.com/documentation/2.6.x/ScalaJsonAutomated
  implicit val iofConfigReader = Json.reads[IofogConfig]
}



/**
 * This object is responsible for holding and updating opcMonitor configurations
 */
object Config {
  val log = Logger(getClass())

  //Set default configs
  log.info("Setting default configs")
  var context = "production"
  var iofogConfig: Option[IofogConfig] = None

  /**
   * This function updates the configs
   */
  def update(
    newConfig: IofogConfig
  ): Unit = {
    log.info("Updating configs")
    log.debug("newConfig: " + newConfig.toString)

    //update configs
    context = "production"
    iofogConfig = Option(newConfig)
  }

  /**
   * This function sets the configs to test values
   * 
   * @param testConfig - IofogConfig instance with configuration to use
   */
  def initTestContext(
    testConfig: IofogConfig
  ): Unit = {
    log.info("Setting test configs")
    log.debug("testConfig: " + testConfig.toString)

    //update configs
    context = "test"
    iofogConfig = Option(testConfig)
  }

  /**
   * This function resets the configs to default values
   */
  def reset: Unit = {
    log.info("Reseting configs")

    //update configs
    context = "production"
    iofogConfig = None
  }
}