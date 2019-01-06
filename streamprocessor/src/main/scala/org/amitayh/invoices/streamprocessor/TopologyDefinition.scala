package org.amitayh.invoices.streamprocessor

import org.apache.kafka.streams.StreamsBuilder

trait TopologyDefinition {
  def setupTopology(builder: StreamsBuilder): StreamsBuilder
}
