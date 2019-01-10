package org.amitayh.invoices.commandhandler

import java.time.Instant
import java.time.LocalDate.now
import java.util.UUID.randomUUID
import java.util.{Properties, UUID}

import org.amitayh.invoices.commandhandler.CommandHandlerTopologyDefinitionSpec._
import org.amitayh.invoices.commandhandler.CommandHandlerTopologyDefinitionSpec.{aCreateInvoiceCommand, aDeleteInvoiceCommand, anEmptyInvoiceSnapshot}
import org.amitayh.invoices.common.Config
import org.amitayh.invoices.common.domain.Command.{CreateInvoice, DeleteInvoice}
import org.amitayh.invoices.common.domain.Event.{InvoiceCreated, InvoiceDeleted}
import org.amitayh.invoices.common.domain._
import org.amitayh.invoices.common.serde.AvroSerde.{CommandResultSerde, CommandSerde, EventSerde, SnapshotSerde}
import org.amitayh.invoices.common.serde.{UuidDeserializer, UuidSerializer}
import org.apache.kafka.streams.StreamsConfig
import org.scalatest.{FunSpec, Matchers, OptionValues}

class CommandHandlerTopologyDefinitionSpec extends FunSpec with TopologyTest with Matchers with OptionValues{

  override def topologyDefinitionUnderTest = CommandHandlerTopologyDefinition

  describe("The Command Handler") {

    val commandTopic = InputTopic[UUID, Command](Config.Topics.Commands.name, UuidSerializer, CommandSerde.serializer)
    val commandResultTopic = OutputTopic[UUID, CommandResult](Config.Topics.CommandResults.name, UuidDeserializer, CommandResultSerde.deserializer)
    val eventTopic: OutputTopic[UUID, Event] = OutputTopic[UUID, Event](Config.Topics.Events.name, UuidDeserializer, EventSerde.deserializer)
    val snapshotTopic = OutputTopic[UUID, InvoiceSnapshot](Config.Topics.Snapshots.name, UuidDeserializer, SnapshotSerde.deserializer)
    val snapshotStore: KVStore[UUID, InvoiceSnapshot] = KVStore(Config.Stores.Snapshots)

    describe("when receiving a Create Invoice command") {

      val command = aCreateInvoiceCommand
      val invoiceId = randomUUID

      it("should emit a successful CommandResult, an InvoiceCreated event and an InvoiceSnapshot, with version=1 and InvoiceState=New") {

        test(exactlyOnce)( driver => {

          commandTopic.pipeIn(driver)(invoiceId, command)

          val commandResultRecord = commandResultTopic.popOut(driver)
          val eventRecord = eventTopic.popOut(driver)
          val snapshotRecord = snapshotTopic.popOut(driver)


          val commandResult: CommandResult = commandResultRecord.value.value
          commandResult.outcome shouldBe a[CommandResult.Success]


          val event: Event = eventRecord.value.value
          event.commandId should be(command.commandId)
          event.version should be(1)
          event.payload shouldBe a[InvoiceCreated]


          val invoiceSnapshot: InvoiceSnapshot = snapshotRecord.value.value
          invoiceSnapshot.version should be(1)
          invoiceSnapshot.invoice.status should be(InvoiceStatus.New)


          assert(commandResultTopic.popOut(driver) isEmpty, "No more CommandResults expected")
          assert(eventTopic.popOut(driver) isEmpty, "No more Events expected")
          assert(snapshotTopic.popOut(driver) isEmpty, "No more InvoiceSnapshots expected")
        })

      }

      it("should add an entry to the state store, with status 'New' and version = 1") {
        test(exactlyOnce)( driver => {

          // Record not there
          val snapshotStateBefore = snapshotStore.get(driver)(invoiceId)
          snapshotStateBefore should not be ('defined)

          commandTopic.pipeIn(driver)(invoiceId, command)

          // Now the Record is there
          val snapshotStateAfter = snapshotStore.get(driver)(invoiceId)
          snapshotStateAfter.value.invoice.status should be (InvoiceStatus.New)
          snapshotStateAfter.value.version should be (1)
        })
      }
    }

    describe("when receiving a DeleteInvoice command") {

      val initialVersion = 1
      val initialInvoiceSnapshot = anEmptyInvoiceSnapshot(initialVersion, InvoiceStatus.New)
      val invoiceId = randomUUID
      val command = aDeleteInvoiceCommand(Option(initialVersion))

      describe("on an existing invoice") {
        it("should emit an InvoiceSnapshot, a successful CommandResult and an InvoiceDeleted event with status 'Deleted' and version incremented by 1") {

          test(exactlyOnce)((driver ) => {
            snapshotStore.put(driver)(invoiceId, initialInvoiceSnapshot)

            commandTopic.pipeIn(driver)(invoiceId, command)

            val commandResultRecord = commandResultTopic.popOut(driver)
            val eventRecord = eventTopic.popOut(driver)
            val snapshotRecord = snapshotTopic.popOut(driver)


            val invoiceSnapshot: InvoiceSnapshot = snapshotRecord.value.value
            invoiceSnapshot.version should be(initialVersion + 1)
            invoiceSnapshot.invoice.status should be(InvoiceStatus.Deleted)

            val commandResult: CommandResult = commandResultRecord.value.value
            commandResult.outcome shouldBe a[CommandResult.Success]


            val event: Event = eventRecord.value.value
            event.commandId should be(command.commandId)
            event.version should be(initialVersion + 1)
            event.payload shouldBe a[InvoiceDeleted]
          })
        }

        it("should update the state store with an InvoiceSnapshot in status=Deleted and version incremented by 1") {
          test(exactlyOnce)((driver ) => {
            snapshotStore.put(driver)(invoiceId, initialInvoiceSnapshot)

            commandTopic.pipeIn(driver)(invoiceId, command)

            val snapshotStateAfter = snapshotStore.get(driver)(invoiceId)
            snapshotStateAfter.value.invoice.status should be (InvoiceStatus.Deleted)
            snapshotStateAfter.value.version should be (initialVersion + 1)
          })
        }
      }

    }
  }
}

object CommandHandlerTopologyDefinitionSpec {

  val exactlyOnce = {
    val props = new Properties
    props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE)
    props
  }

  def aCreateInvoiceCommand: Command = Command(
    originId = randomUUID,
    commandId = randomUUID,
    expectedVersion = None,
    CreateInvoice("a-customer", "a.customer@ema.il", now, now plusDays 1, List())
  )

  def aDeleteInvoiceCommand(expVersion: Option[Int]): Command = Command(
    originId = randomUUID,
    commandId = randomUUID,
    expectedVersion = expVersion,
    DeleteInvoice()
  )

  def anEmptyInvoiceSnapshot(version: Int, status: InvoiceStatus): InvoiceSnapshot = InvoiceSnapshot(
    Invoice(
      Customer("a-customer", "a.customer@ema.il"),
      now,
      now plusDays 1,
      Vector(),
      status,
      0.00
    ),
    version,
    Instant.now
  )

}