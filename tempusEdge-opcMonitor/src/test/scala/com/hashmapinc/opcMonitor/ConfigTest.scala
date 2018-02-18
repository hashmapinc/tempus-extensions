package com.hashmapinc.opcMonitor

import org.scalatest.FlatSpec

class ConfigTest extends FlatSpec {

  "Config" should "start with no config" in {
    assert(!Config.iofogConfig.isDefined)
  }

  it should "start with 'production' context" in {
    assert(Config.context == "production")
  }

  it should "accept and retain updates" in {
    val opcHost = "opcHost"
    val opcPort = 80

    val newConfig = new IofogConfig(
      opcHost,
      opcPort
    )

    Config.update(newConfig)

    assert(Config.iofogConfig.get.opcHost == opcHost)
    assert(Config.iofogConfig.get.opcPort == opcPort)
    assert(Config.context == "production")
  }

  it should "accept and retain test configs" in {
    val opcHost = "opcHost"
    val opcPort = 80

    val newConfig = new IofogConfig(
      opcHost,
      opcPort
    )

    Config.initTestContext(newConfig)

    assert(Config.iofogConfig.get.opcHost == opcHost)
    assert(Config.iofogConfig.get.opcPort == opcPort)
    assert(Config.context == "test")
  }

  it should "reset to default configs when 'reset' is invoked" in {
    Config.reset
    assert(Config.context == "production")
    assert(!Config.iofogConfig.isDefined)
  }
}