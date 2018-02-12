/*
 * Copyright 2001-2009 Artima, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hashmapinc.opcMonitor

import org.scalatest.FlatSpec

class OpcMonitorTest extends FlatSpec {

  "testable" should "add correctly" in {
    val a = 3
    val b = 3
    val c = a + b
    val d = OpcMonitor.testable(a,b)
    assert(c == d)
  }

  it should "still add correctly" in {
    val a = 5
    val b = 12
    val c = a + b
    val d = OpcMonitor.testable(a,b)
    assert(c == d)
  }
}