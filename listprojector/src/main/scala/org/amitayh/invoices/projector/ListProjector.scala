package org.amitayh.invoices.projector

import org.amitayh.invoices.streamprocessor.StreamProcessorApp

object ListProjector extends StreamProcessorApp with ListProjectorTopologyDefinition {

  override def appId: String = "invoices.processor.list-projector"

}


