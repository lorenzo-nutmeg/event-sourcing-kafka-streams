package org.amitayh.invoices.projector

import java.util.UUID

import cats.effect.IO
import org.amitayh.invoices.common.Config
import org.amitayh.invoices.common.domain.InvoiceSnapshot
import org.amitayh.invoices.common.serde.AvroSerde.SnapshotSerde
import org.amitayh.invoices.common.serde.UuidSerde
import org.amitayh.invoices.dao.{InvoiceList, InvoiceRecord}
import org.amitayh.invoices.streamprocessor.TopologyDefinition
import org.apache.kafka.streams.{KeyValue, StreamsBuilder}
import org.apache.kafka.streams.kstream.{Consumed, ForeachAction, KeyValueMapper}

trait ListProjectorTopologyDefinition extends TopologyDefinition {
  def topology(builder: StreamsBuilder ): StreamsBuilder = {
    val snapshots = builder.stream(
      Config.Topics.Snapshots.name,
      Consumed.`with`(UuidSerde, SnapshotSerde))

    snapshots
      .map[UUID, InvoiceRecord](ToRecord)
      .foreach(SaveInvoiceRecord)
  }
}

object ToRecord extends KeyValueMapper[UUID, InvoiceSnapshot, KeyValue[UUID, InvoiceRecord]] {
  override def apply(id: UUID, snapshot: InvoiceSnapshot): KeyValue[UUID, InvoiceRecord] =
    KeyValue.pair(id, InvoiceRecord(id, snapshot))
}

object SaveInvoiceRecord extends ForeachAction[UUID, InvoiceRecord] {
  override def apply(id: UUID, value: InvoiceRecord): Unit =
    InvoiceList[IO].save(value).unsafeRunSync()
}