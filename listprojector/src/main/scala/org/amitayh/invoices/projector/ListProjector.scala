package org.amitayh.invoices.projector

import org.amitayh.invoices.streamprocessor.{StreamProcessorApp, TopologyDefinition}

object ListProjector extends StreamProcessorApp {

  override def appId: String = "invoices.processor.list-projector"

  override def topologyDefinition: TopologyDefinition = ListProjectorTopologyDefinition

}


