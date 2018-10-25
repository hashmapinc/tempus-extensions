package com.hashmap.tempus

import com.hashmap.tempus.StickSlickTorqueCalculator.Data
import com.hashmap.tempus.StickSlickTorqueCalculator.orderFunctionBasedOnTorque
import org.scalatest.FlatSpec

class StickSlickTorqueCalculatorTest extends FlatSpec {


  "StickSlickTorqueCalculator" should "deserialize input data" in {
    //given
    val inputData = s"""{"id":"deviceId", "ts":109898, "currentTorque":780.098}"""
    //when
    val actualData = StickSlickTorqueCalculator.deserialize(inputData)
    //then
    val expectedData = StickSlickTorqueCalculator.Data("deviceId", "109898", 780.098)
    assert(expectedData === actualData)
  }


  "StickSlickTorqueCalculator" should "correctly order based on Torque" in {
    //given
    val givenData = List(Data("givenId", "10909", 890.98), Data("givenId", "10909", 990.98), Data("givenId", "10909", 990.18), Data("givenId", "10909", 895.58))
    //when
    val maxTorque = givenData.max(orderFunctionBasedOnTorque).currentTorque
    val minTorque = givenData.min(orderFunctionBasedOnTorque).currentTorque
    //then
    assert(maxTorque === 990.98)
    assert(minTorque === 890.98)
  }


}
