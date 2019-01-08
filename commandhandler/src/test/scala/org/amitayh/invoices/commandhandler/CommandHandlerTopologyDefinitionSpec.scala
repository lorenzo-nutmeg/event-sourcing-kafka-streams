package org.amitayh.invoices.commandhandler

import java.time.LocalDate.now
import java.util.UUID
import java.util.UUID.randomUUID

import org.amitayh.invoices.commandhandler.CommandHandlerTopologyDefinitionSpec._
import org.amitayh.invoices.common.Config
import org.amitayh.invoices.common.domain.Command._
import org.amitayh.invoices.common.domain.Event.InvoiceCreated
import org.amitayh.invoices.common.domain._
import org.amitayh.invoices.common.serde.AvroSerde.{CommandResultSerde, CommandSerde, EventSerde, SnapshotSerde}
import org.amitayh.invoices.common.serde.{UuidDeserializer, UuidSerializer}
import org.apache.kafka.clients.producer.ProducerRecord
import org.scalatest.{FunSpec, Matchers, OptionValues}


class CommandHandlerTopologyDefinitionSpec extends FunSpec
  with CommandHandlerTopologyDefinition with KafkaStreamTopologyTest
  with Matchers with OptionValues {

  describe("The Command Handler") {
    describe("when receiving a Create Invoice command") {

      val command = aCreateInvoiceCommand
      val invoiceId = randomUUID

      it("should emit a successful CommandResult, an InvoiceCreated event and an InvoiceSnapshot, with version=1 and InvoiceState=New") {
        testTopology(() => {

          val commandTopic = inputTopic[UUID, Command](Config.Topics.Commands.name, UuidSerializer, CommandSerde.serializer)

          val commandResultTopic = outputTopic[UUID, CommandResult](Config.Topics.CommandResults.name, UuidDeserializer, CommandResultSerde.deserializer)
          val eventTopic: OutputTopic[UUID, Event] = outputTopic[UUID, Event](Config.Topics.Events.name, UuidDeserializer, EventSerde.deserializer)
          val snapshotTopic = outputTopic[UUID, InvoiceSnapshot](Config.Topics.Snapshots.name, UuidDeserializer, SnapshotSerde.deserializer)

          commandTopic.pipeIn(invoiceId, command)

          val commandResultRecord = commandResultTopic.receive
          val eventRecord = eventTopic.receive
          val snapshotRecord = snapshotTopic.receive


          val commandResult: CommandResult = commandResultRecord.value.value
          commandResult.outcome shouldBe a[CommandResult.Success]


          val event: Event = eventRecord.value.value
          event.commandId should be(command.commandId)
          event.version should be(1)
          event.payload shouldBe a[InvoiceCreated]


          val invoiceSnapshot: InvoiceSnapshot = snapshotRecord.value.value
          invoiceSnapshot.version should be(1)
          invoiceSnapshot.invoice.status should be(InvoiceStatus.New)


          assert(commandResultTopic.receive isEmpty, "No more CommandResults expected")
          assert(eventTopic.receive.isEmpty, "No more Events expected")
          assert(snapshotTopic.receive isEmpty, "No more InvoiceSnapshots expected")
        })
      }


      it("should add an entry to the state store, with status 'New' and version = 1") {
        testTopology(() => {
          val commandTopic = inputTopic[UUID, Command](Config.Topics.Commands.name, UuidSerializer, CommandSerde.serializer)
          val snapshotStore: KVStore[UUID, InvoiceSnapshot] = keyValueStore(Config.Stores.Snapshots)

          // Record not there
          val snapshotStateBefore = snapshotStore.get(invoiceId)
          snapshotStateBefore should not be ('defined)

          commandTopic.pipeIn(invoiceId, command)

          // Record not there
          val snapshotStateAfter = snapshotStore.get(invoiceId)
          snapshotStateAfter.value.invoice.status should be (InvoiceStatus.New)
          snapshotStateAfter.value.version should be (1)
        })
      }
    }

    describe("when receiving a DeleteInvoice command") {
      describe("on an existing invoice") {
        ignore("should produce an InvoiceSnapshot with status 'Deleted' and version incremented by 1") {
          ???
        }

        ignore("should emit an InvoiceDeleted event, with version incremented by 1") {
          ???
        }

        ignore("should emit a successful CommandResult") {
          ???
        }
      }


    }
  }

}

object CommandHandlerTopologyDefinitionSpec {


  def aCreateInvoiceCommand: Command = Command(
    originId = randomUUID,
    commandId = randomUUID,
    expectedVersion = None,
    CreateInvoice("a-customer", "a.customer@ema.il", now, now plusDays 1, List())
  )

}
