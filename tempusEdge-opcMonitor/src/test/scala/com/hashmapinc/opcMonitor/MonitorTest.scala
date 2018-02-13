package com.hashmapinc.opcMonitor

import org.scalatest.FlatSpec

class MonitorTest extends FlatSpec {

  "testable" should "add correctly" in {
    val a = 3
    val b = 3
    val c = a + b
    val d = Monitor.testable(a, b)
    assert(c == d)
  }

  it should "still add correctly" in {
    val a = 5
    val b = 12
    val c = a + b
    val d = Monitor.testable(a, b)
    assert(c == d)
  }
}