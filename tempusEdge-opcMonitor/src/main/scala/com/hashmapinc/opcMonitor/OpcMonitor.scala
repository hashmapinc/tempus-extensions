package com.hashmapinc.opcMonitor

import org.eclipse.milo.opcua.sdk.client.OpcUaClient
import org.eclipse.milo.opcua.sdk.client.api.identity.AnonymousProvider
import org.eclipse.milo.opcua.sdk.client.api.identity.IdentityProvider
import org.eclipse.milo.opcua.stack.core.security.SecurityPolicy

/**
 * @author randypitcherii
 */
object OpcMonitor {
  
  def foo(x : Array[String]) = x.foldLeft("")((a,b) => a + b)

  def testable(a: Int, b: Int): Int = a + b
  
  def main(args : Array[String]) {
    println( "Hello World!" )
    println("concat arguments = " + foo(args))
  }

}
