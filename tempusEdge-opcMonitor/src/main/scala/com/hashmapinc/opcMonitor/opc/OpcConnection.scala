package com.hashmapinc.opcMonitor.opc

import java.io.File

import org.eclipse.milo.opcua.sdk.client.OpcUaClient
import org.eclipse.milo.opcua.sdk.client.api.config.{OpcUaClientConfig, OpcUaClientConfigBuilder}
import org.eclipse.milo.opcua.sdk.client.api.identity.AnonymousProvider
import org.eclipse.milo.opcua.stack.core.types.builtin.LocalizedText
import org.eclipse.milo.opcua.stack.core.types.builtin.unsigned.Unsigned.uint

import com.typesafe.scalalogging.Logger

import com.hashmapinc.opcMonitor.Config

object OpcConnection {
  private val logger = Logger(getClass())

  //create client
  logger.info("creating opc client")
  var client = new OpcUaClient(getClientConfig)
  logger.info("opc client created...")

  /**
   * Uses values in the Config object to create an opcUaClient configuration
   */
  def getClientConfig: OpcUaClientConfig = {
    logger.info("creating opc client configuration")
    // TODO: do this properly
    OpcUaClientConfig.builder()
      .setApplicationName(LocalizedText.english("Tempus Edge - OPC Client"))
      .setApplicationUri("urn:hashmapinc:tempus:edge:opcClient")
      .setIdentityProvider(new AnonymousProvider())
      .setRequestTimeout(uint(5000))
      .build()
  }

  /**
   * This function subscribs the opc client to the tag given
   *
   * @param tag - String value of the tag to subscribe to
   */
  def subscribe(
    tag: String
  ): Unit = {

  }
}