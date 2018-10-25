package com.hashmapinc.tempus

import org.scalatest.FlatSpec

class BlockDirectionAndVelocityCalculatorTest extends FlatSpec{

  "BlockDirectionAndVelocityCalculator" should "parse incoming json into BlockPositionData and pair it with device id" in {
    val inputStr = """{"id":"Block Position 123", "ts":"1539926210702", "blockPosition":37.20034722222222}"""
    val expectedResult = ("Block Position 123", BlockPositionData("Block Position 123", 1539926210702L, 37.20034722222222))
    val actualResult = BlockDirectionAndVelocityCalculator.parseAndPairByKey(inputStr)
    assert(expectedResult === actualResult)
  }

  "it" should "calculate velocity from previous and current position data" in {
    val previousPosData = BlockPositionData("Device 1", 1539926310000L, 40.30)
    val currentPosData = BlockPositionData("Device 1", 1539926320000L, 35.30)
    assert(BlockDirectionAndVelocityCalculator.calculateVelocity(previousPosData, currentPosData) === 0.5)
  }

  "it" should "calculate upward direction from previous and current position data" in {
    val previousPosData = BlockPositionData("Device 1", 1539926310000L, 40.30)
    val currentPosData = BlockPositionData("Device 1", 1539926320000L, 35.30)
    assert(BlockDirectionAndVelocityCalculator.calculateDirection(previousPosData, currentPosData) === (-1, "down"))
  }

  "it" should "calculate downward direction from previous and current position data" in {
    val previousPosData = BlockPositionData("Device 1", 1539926310000L, 35.30)
    val currentPosData = BlockPositionData("Device 1", 1539926320000L, 40.30)
    assert(BlockDirectionAndVelocityCalculator.calculateDirection(previousPosData, currentPosData) === (1, "up"))
  }

  "it" should "calculate stopped direction from previous and current position data" in {
    val previousPosData = BlockPositionData("Device 1", 1539926310000L, 40.30)
    val currentPosData = BlockPositionData("Device 1", 1539926320000L, 40.30)
    assert(BlockDirectionAndVelocityCalculator.calculateDirection(previousPosData, currentPosData) === (0, "stopped"))
  }
}
