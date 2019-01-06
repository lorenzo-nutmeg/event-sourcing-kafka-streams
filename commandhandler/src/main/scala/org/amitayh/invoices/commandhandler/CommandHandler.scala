package org.amitayh.invoices.commandhandler

import org.amitayh.invoices.streamprocessor.StreamProcessorApp

object CommandHandler extends StreamProcessorApp with CommandHandlerTopologyDefinition {

  override def appId: String = "invoices.processor.command-handler"

}

