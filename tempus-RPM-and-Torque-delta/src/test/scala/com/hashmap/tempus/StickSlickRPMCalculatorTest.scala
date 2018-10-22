package com.hashmap.tempus

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


//  "StickSlickRpmCalculator" should "find min and max" in {
//
//  }



}
