package org.amitayh.invoices.commandhandler

import java.time.LocalDate.now
import java.util.UUID
import java.util.UUID.randomUUID

import com.madewithtea.mockedstreams.MockedStreams
import org.amitayh.invoices.common.Config
import org.amitayh.invoices.common.domain._
import org.amitayh.invoices.common.domain.Command.{CreateInvoice, DeleteInvoice}
import org.amitayh.invoices.common.serde.UuidSerde
import org.scalatest.{FunSpec, Matchers, OptionValues}
import org.amitayh.invoices.common.serde.AvroSerde._
import org.amitayh.invoices.common.domain.Event.{InvoiceCreated, InvoiceDeleted}
import CommandHandlerTopologyDefinitionSpec._
import org.apache.kafka.streams.state.Stores

import scala.collection.immutable.Stream.StreamBuilder
import java.time.Instant

class CommandHandlerTopologyDefinitionSpec extends FunSpec with CommandHandlerTopologyDefinition with Matchers with OptionValues {
  describe("The Command Handler") {

    describe("when receiving a CreateInvoice command") {
      val invoiceId = randomUUID
      val command = aCreateInvoiceCommand

      val commands = Seq((invoiceId, command))
      val mockedStreams = MockedStreams()
        .topology(setupTopology)
        .input(Config.Topics.Commands.name, UuidSerde, CommandSerde,commands)
        .stores(Seq(Config.Stores.Snapshots))

      it("should emit a single InvoiceCreated event, with version=1") {

        val events = mockedStreams.output(Config.Topics.Events.name, UuidSerde, EventSerde, 1)

        val eventRecord: (UUID,Event) = events(0)
        eventRecord._1 should be (invoiceId)
        eventRecord._2.version should be (1)
        eventRecord._2.payload shouldBe a[InvoiceCreated]


      }

      it("should store the Snapshot, with status=New and version=1") {
        val snapshotStore = mockedStreams.stateTable(Config.Stores.Snapshots).asInstanceOf[Map[UUID,InvoiceSnapshot]]

        val maybeSnapshot: Option[InvoiceSnapshot] = snapshotStore.get(invoiceId)
        maybeSnapshot.value.version should be (1)
        maybeSnapshot.value.invoice.status should be (InvoiceStatus.New)
      }

      it("should produce a single InvoiceSnapshot with status=New and version=1") {
        val invoiceSnapshots = mockedStreams.output(Config.Topics.Snapshots.name, UuidSerde, SnapshotSerde, 1)

        val invoiceSnapshotRecord: (UUID, InvoiceSnapshot) = invoiceSnapshots(0)
        invoiceSnapshotRecord._1 should be (invoiceId)
        invoiceSnapshotRecord._2.version should be (1)
        invoiceSnapshotRecord._2.invoice.status should be (InvoiceStatus.New)
      }

      it("should produce a single successful CommandResult") {
        val commandResults = mockedStreams.output(Config.Topics.CommandResults.name, UuidSerde, CommandResultSerde, 1)

        val commandResultRecord: (UUID, CommandResult) = commandResults(0)
        commandResultRecord._1 should be (invoiceId)
        commandResultRecord._2.outcome shouldBe a[CommandResult.Success]
      }
    }


    describe("when receiving a DeleteInvoice command for an existing Invoice") {
      val invoiceId = randomUUID
      val currentVersion = 42
      val currentInvoice = aNewEmptyInvoice
      val currentSnapshot = invoiceSnapshot(currentInvoice, currentVersion)

      val command = aDeleteInvoiceCommand(invoiceId, Some(currentVersion))

      val commands = Seq((invoiceId, command))

      // trying to following the suggestions by https://github.com/jpzk/mockedstreams/issues/30, but not sure...
      val stateSupplierBuilder = MockedStreams()
        .topology( builder => builder.addStateStore(
          Stores.keyValueStoreBuilder(
            Stores.persistentKeyValueStore(Config.Stores.Snapshots),
            UuidSerde,
            SnapshotSerde)))
        .stores(Seq(Config.Stores.Snapshots))
        .output(Config.Stores.Snapshots, UuidSerde, SnapshotSerde, 1)

      val mockedStreams = MockedStreams()
        .topology(setupTopology)
        .input(Config.Stores.Snapshots, UuidSerde, SnapshotSerde, Seq((invoiceId, currentSnapshot)))
        .input(Config.Topics.Commands.name, UuidSerde, CommandSerde,commands)
        .stores(Seq(Config.Stores.Snapshots))

      it("should update the store with a Snapshot with state=Deleted and version incremented by 1") {
        ???
      }

      it("should produce a single InvoiceSnapshot with status=Deleted and version incremented by 1") {
        ???
      }

      it("should emit a single InvoiceDeleted event") {
        ???
      }

      it("should produce a single successful CommandResult") {
        ???
      }

    }

    // TODO Test other types of Commands
  }
}

object CommandHandlerTopologyDefinitionSpec {
  def aCreateInvoiceCommand: Command = Command(
    originId = randomUUID,
    commandId = randomUUID,
    expectedVersion = None,
    payload = CreateInvoice("a-customer", "a.customer@ema.il", now, now plusDays 1, List())
  )

  def aDeleteInvoiceCommand(invoiceId: UUID, expectedVersion: Option[Int]): Command  = Command(
    originId = randomUUID,
    commandId = randomUUID,
    expectedVersion = expectedVersion,
    payload = DeleteInvoice()
  )

  def aNewEmptyInvoice: Invoice = Invoice(
    customer = Customer("a-customer", "a.customer@ema.il"),
    issueDate = now,
    dueDate = now plusDays 1,
    lineItems = Vector(),
    status = InvoiceStatus.New,
    null
  )

  def invoiceSnapshot(invoice: Invoice, currentVersion: Int): InvoiceSnapshot = InvoiceSnapshot(
    invoice = invoice,
    version = currentVersion,
    Instant.now
  )

}
