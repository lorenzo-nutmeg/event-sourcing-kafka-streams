package org.amitayh.invoices.commandhandler

import java.time.LocalDate.now
import java.util.UUID
import java.util.UUID.randomUUID

import com.madewithtea.mockedstreams.MockedStreams
import org.amitayh.invoices.common.Config
import org.amitayh.invoices.common.domain.{Command, Event, InvoiceSnapshot, InvoiceStatus, CommandResult}
import org.amitayh.invoices.common.domain.Command.CreateInvoice
import org.amitayh.invoices.common.serde.UuidSerde
import org.scalatest.{FunSpec, Matchers, OptionValues}
import org.amitayh.invoices.common.serde.AvroSerde._
import org.amitayh.invoices.common.domain.Event.InvoiceCreated

import  CommandHandlerTopologyDefinitionSpec._

class CommandHandlerTopologyDefinitionSpec extends FunSpec with CommandHandlerTopologyDefinition with Matchers with OptionValues {
  describe("The Command Handler") {
    describe("when receiving a CreateInvoice command") {
      val invoiceId = randomUUID
      val originId = randomUUID
      val command = aCreateInvoiceCommand(originId)

      val commands = Seq((invoiceId, command))
      val mockedStreams = MockedStreams()
        .topology(setupTopology)
        .input(Config.Topics.Commands.name, UuidSerde, CommandSerde,commands)
        .stores(Seq(Config.Stores.Snapshots))

      it("should generate a single InvoiceCreated event, with version=1") {

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

      it("should produce a single Invoice Snapshot with status=New and version=1") {
        val invoiceSnapshots = mockedStreams.output(Config.Topics.Snapshots.name, UuidSerde, SnapshotSerde, 1)

        val invoiceSnapshotRecord: (UUID, InvoiceSnapshot) = invoiceSnapshots(0)
        invoiceSnapshotRecord._1 should be (invoiceId)
        invoiceSnapshotRecord._2.version should be (1)
        invoiceSnapshotRecord._2.invoice.status should be (InvoiceStatus.New)
      }

      it("should produce a single successful Command Result") {
        val commandResults = mockedStreams.output(Config.Topics.CommandResults.name, UuidSerde, CommandResultSerde, 1)

        val commandResultRecord: (UUID, CommandResult) = commandResults(0)
        commandResultRecord._1 should be (invoiceId)
        commandResultRecord._2.outcome shouldBe a[CommandResult.Success]
      }
    }

    // TODO Test other types of Commands
  }
}

object CommandHandlerTopologyDefinitionSpec {
  def aCreateInvoiceCommand(originId: UUID): Command = Command(
    originId = originId,
    commandId = randomUUID,
    expectedVersion = None,
    CreateInvoice("a-customer", "a.customer@ema.il", now, now plusDays 1, List())
  )

}
