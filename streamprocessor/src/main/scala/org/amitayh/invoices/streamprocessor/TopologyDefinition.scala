package org.amitayh.invoices.streamprocessor

import org.apache.kafka.streams.Topology

trait TopologyDefinition {
  def topology: Topology
}
