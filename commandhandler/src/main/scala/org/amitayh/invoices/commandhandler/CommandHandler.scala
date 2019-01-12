package org.amitayh.invoices.commandhandler

import org.amitayh.invoices.streamprocessor.{StreamProcessorApp, TopologyDefinition}

object CommandHandler extends StreamProcessorApp  {

  override def appId: String = "invoices.processor.command-handler"

  override def topologyDefinition: TopologyDefinition = CommandHandlerTopologyDefinition
}



