package com.hashmapinc.tempus

import org.scalatest.FlatSpec

class RigStateCalculatorTest extends FlatSpec {

  "RigStateCalculator" should "parse incoming json into RigStateData and pair it with device id" in {
    val inputStr = """{"id":"Rig State 123", "ts":"1539926210702", "bitDepth":37.20034722222222, "holeDepth":89.3423, "totalPumpOutput":200, "rotaryRpm":150, "bitOnBottom":1, "inSlipStatus":0, "standpipePressure":243}"""
    val expectedResult = ("Rig State 123", RigStateData("Rig State 123", 1539926210702L, 37.20034722222222, 89.3423, 200, 150, 0, 243))
    val actualResult = RigStateCalculator.parseAndPairByKey(inputStr)
    assert(expectedResult === actualResult)
  }

  "it" should "calculate rig state data and return Depth Data Issue" in {
    val currentRigStateData = RigStateData("Rig State 123", 1539926210702L, -999.25, -999.25, 200, 150, 0, 243)
    assert(RigStateCalculator.calculateRigState(null, currentRigStateData) == "Depth Data Issue")
  }

  "it" should "calculate rig state and return Out of Hole" in {
    val currentRigStateData = RigStateData("Rig State 123", 1539926210702L, 0, 100, 200, 150, 0, 243)
    assert(RigStateCalculator.calculateRigState(null, currentRigStateData) == "Out of Hole")
  }

  "it" should "calculate rig state and return Stationary" in {
    val previousRigStateData = RigStateData("Rig State 123", 1539926210700L, 10.1, 123, 80, 45, 0, 100)
    val currentRigStateData = RigStateData("Rig State 123", 1539926210702L, 10, 123, 80, 5, 0, 100)
    assert(RigStateCalculator.calculateRigState(previousRigStateData, currentRigStateData) == "Stationary")
  }

  "it" should "calculate rig state and return Rotating" in {
    val previousRigStateData = RigStateData("Rig State 123", 1539926210700L, 10.1, 123, 80, 45, 0, 100)
    val currentRigStateData = RigStateData("Rig State 123", 1539926210702L, 10, 123, 80, 35, 0, 100)
    assert(RigStateCalculator.calculateRigState(previousRigStateData, currentRigStateData) == "Rotating")
  }

  "it" should "calculate rig state and return Rotating Pumping" in {
    val previousRigStateData = RigStateData("Rig State 123", 1539926210700L, 10.1, 123, 80, 45, 0, 100)
    val currentRigStateData = RigStateData("Rig State 123", 1539926210702L, 10, 123, 120, 45, 0, 508)
    assert(RigStateCalculator.calculateRigState(previousRigStateData, currentRigStateData) == "Rotating Pumping")
  }

  "it" should "calculate rig state and return Pumping" in {
    val previousRigStateData = RigStateData("Rig State 123", 1539926210700L, 10.1, 123, 80, 45, 0, 100)
    val currentRigStateData = RigStateData("Rig State 123", 1539926210702L, 10, 123, 159, 8, 0, 527)
    assert(RigStateCalculator.calculateRigState(previousRigStateData, currentRigStateData) == "Pumping")
  }

  "it" should "calculate rig state and return Rotary Drilling" in {
    val previousRigStateData = RigStateData("Rig State 123", 1539926210700L, 121, 123, 80, 45, 0, 100)
    val currentRigStateData = RigStateData("Rig State 123", 1539926210702L, 122.8, 123, 159, 68, 0, 527)
    assert(RigStateCalculator.calculateRigState(previousRigStateData, currentRigStateData) == "Rotary Drilling")
  }

  "it" should "calculate rig state and return Slide Drilling" in {
    val previousRigStateData = RigStateData("Rig State 123", 1539926210700L, 121, 123, 80, 45, 0, 100)
    val currentRigStateData = RigStateData("Rig State 123", 1539926210702L, 122.86, 123, 159, 7, 0, 527)
    assert(RigStateCalculator.calculateRigState(previousRigStateData, currentRigStateData) == "Slide Drilling")
  }

  "it" should "calculate rig state and return Tripping Out Pumping" in {
    val previousRigStateData = RigStateData("Rig State 123", 1539926210700L, 22, 123, 80, 45, 0, 100)
    val currentRigStateData = RigStateData("Rig State 123", 1539926210702L, 15, 123, 159, 7, 0, 527)
    assert(RigStateCalculator.calculateRigState(previousRigStateData, currentRigStateData) == "Tripping Out Pumping")
  }

  "it" should "calculate rig state and return Tripping Out" in {
    val previousRigStateData = RigStateData("Rig State 123", 1539926210700L, 22, 123, 80, 45, 0, 100)
    val currentRigStateData = RigStateData("Rig State 123", 1539926210702L, 15, 123, 59, 7, 0, 527)
    assert(RigStateCalculator.calculateRigState(previousRigStateData, currentRigStateData) == "Tripping Out")
  }

  "it" should "calculate rig state and return Tripping Out Rotating" in {
    val previousRigStateData = RigStateData("Rig State 123", 1539926210700L, 22, 123, 80, 45, 0, 100)
    val currentRigStateData = RigStateData("Rig State 123", 1539926210702L, 15, 123, 59, 87, 0, 527)
    assert(RigStateCalculator.calculateRigState(previousRigStateData, currentRigStateData) == "Tripping Out Rotating")
  }

  "it" should "calculate rig state and return Tripping In Pumping" in {
    val previousRigStateData = RigStateData("Rig State 123", 1539926210700L, 22, 123, 80, 45, 0, 100)
    val currentRigStateData = RigStateData("Rig State 123", 1539926210702L, 85, 123, 159, 8, 0, 527)
    assert(RigStateCalculator.calculateRigState(previousRigStateData, currentRigStateData) == "Tripping In Pumping")
  }

  "it" should "calculate rig state and return Tripping In" in {
    val previousRigStateData = RigStateData("Rig State 123", 1539926210700L, 22, 123, 80, 45, 0, 100)
    val currentRigStateData = RigStateData("Rig State 123", 1539926210702L, 85, 123, 19, 8, 0, 527)
    assert(RigStateCalculator.calculateRigState(previousRigStateData, currentRigStateData) == "Tripping In")
  }

  "it" should "calculate rig state and return Tripping In Rotating" in {
    val previousRigStateData = RigStateData("Rig State 123", 1539926210700L, 22, 123, 80, 45, 0, 100)
    val currentRigStateData = RigStateData("Rig State 123", 1539926210702L, 85, 123, 19, 88, 0, 527)
    assert(RigStateCalculator.calculateRigState(previousRigStateData, currentRigStateData) == "Tripping In Rotating")
  }

  "it" should "calculate rig state and return In Slips" in {
    val previousRigStateData = RigStateData("Rig State 123", 1539926210700L, 22, 123, 80, 45, 0, 100)
    val currentRigStateData = RigStateData("Rig State 123", 1539926210702L, 85, 123, 19, 88, 1, 527)
    assert(RigStateCalculator.calculateRigState(previousRigStateData, currentRigStateData) == "In Slips")
  }

  "it" should "calculate rig state and return Back Ream" in {
    val previousRigStateData = RigStateData("Rig State 123", 1539926210700L, 102, 123, 80, 45, 0, 100)
    val currentRigStateData = RigStateData("Rig State 123", 1539926210702L, 85, 123, 129, 88, 0, 527)
    assert(RigStateCalculator.calculateRigState(previousRigStateData, currentRigStateData) == "Back Ream")
  }

  "it" should "calculate rig state and return Ream In" in {
    val previousRigStateData = RigStateData("Rig State 123", 1539926210700L, 102, 123, 80, 45, 0, 100)
    val currentRigStateData = RigStateData("Rig State 123", 1539926210702L, 112, 123, 129, 88, 0, 527)
    assert(RigStateCalculator.calculateRigState(previousRigStateData, currentRigStateData) == "Ream In")
  }
}