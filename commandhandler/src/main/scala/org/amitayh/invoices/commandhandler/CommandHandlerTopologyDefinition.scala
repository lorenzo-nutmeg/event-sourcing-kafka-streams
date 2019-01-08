package org.amitayh.invoices.commandhandler

import org.amitayh.invoices.common.Config
import org.amitayh.invoices.common.domain.{CommandResult, Event, InvoiceSnapshot}
import org.amitayh.invoices.common.serde.AvroSerde.{CommandResultSerde, CommandSerde, EventSerde, SnapshotSerde}
import org.amitayh.invoices.common.serde.UuidSerde
import org.amitayh.invoices.streamprocessor.TopologyDefinition
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.kstream.{Consumed, Produced}
import org.apache.kafka.streams.state.Stores

// Extracted topology definition to allow testing
trait CommandHandlerTopologyDefinition extends TopologyDefinition {
  def topology(builder: StreamsBuilder ): StreamsBuilder = {

    builder.addStateStore(
      Stores.keyValueStoreBuilder(
        Stores.persistentKeyValueStore(Config.Stores.Snapshots),
        UuidSerde,
        SnapshotSerde))

    val commands = builder.stream(
      Config.Topics.Commands.name,
      Consumed.`with`(UuidSerde, CommandSerde))

    val results = commands.transform(
      CommandToResultTransformer.Supplier,
      Config.Stores.Snapshots)

    val successfulResults = results.flatMapValues[CommandResult.Success](ToSuccessful)
    val snapshots = successfulResults.mapValues[InvoiceSnapshot](ToSnapshots)
    val events = successfulResults.flatMapValues[Event](ToEvents)

    results.to(
      Config.Topics.CommandResults.name,
      Produced.`with`(UuidSerde, CommandResultSerde))

    snapshots.to(
      Config.Topics.Snapshots.name,
      Produced.`with`(UuidSerde, SnapshotSerde))

    events.to(
      Config.Topics.Events.name,
      Produced.`with`(UuidSerde, EventSerde))

    builder
  }
}
