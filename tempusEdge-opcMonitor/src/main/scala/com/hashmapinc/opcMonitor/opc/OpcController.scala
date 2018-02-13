package com.hashmapinc.opcMonitor.opc

import org.eclipse.milo.opcua.sdk.client.OpcUaClient
import org.eclipse.milo.opcua.sdk.client.api.identity.AnonymousProvider
import org.eclipse.milo.opcua.sdk.client.api.identity.IdentityProvider
import org.eclipse.milo.opcua.stack.core.security.SecurityPolicy

import com.typesafe.scalalogging.Logger

object OpcController {
  val logger = Logger(getClass())

}