package com.hashmapinc.opcMonitor

import com.hashmapinc.opcMonitor.iofog.IofogController
import com.hashmapinc.opcMonitor.mqtt.MqttController
import com.hashmapinc.opcMonitor.opc.OpcController

/**
 * Driver for the overall OPC Monitoring process
 *
 * @author randypitcherii
 */
object Monitor {

  def foo(x: Array[String]) = x.foldLeft("")((a, b) => a + b)

  def testable(a: Int, b: Int): Int = a + b

  def main(args: Array[String]) {
    println("Hello World!")
    println("concat arguments = " + foo(args))
  }

}
