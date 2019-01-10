package org.amitayh.invoices.streamprocessor

import org.apache.kafka.streams.StreamsBuilder

abstract class TopologyDefinition {

  def apply(builder: StreamsBuilder): StreamsBuilder
}
