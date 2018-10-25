package com.hashmap.tempus

import com.hashmap.tempus.StickSlickRPMCalculator.Data
import com.hashmap.tempus.StickSlickRPMCalculator.orderFunctionBasedOnRPM
import org.scalatest.FlatSpec

class StickSlickRPMCalculatorTest extends FlatSpec{

  "StickSlickRPMCalculator" should "deserialize input data" in {
    //given
    val inputData = s"""{"id":"deviceId", "ts":109898, "currentRpm":25.098}"""
    //when
    val actualData = StickSlickRPMCalculator.deserialize(inputData)
    //then
    val expectedData = StickSlickRPMCalculator.Data("deviceId", "109898", 25.098)
    assert(expectedData === actualData)
  }


  "StickSlickRPMCalculator" should "correctly order based on RPM" in {
    //given
    val givenData = List(Data("givenId", "10909", 19.098), Data("givenId", "10909", 19.056), Data("givenId", "10909", 18.098), Data("givenId", "10909", 18.998))
    //when
    val maxRpm = givenData.max(orderFunctionBasedOnRPM).currentRpm
    val minRpm = givenData.min(orderFunctionBasedOnRPM).currentRpm
    //then
    assert(maxRpm === 19.098)
    assert(minRpm === 18.098)
  }
}
