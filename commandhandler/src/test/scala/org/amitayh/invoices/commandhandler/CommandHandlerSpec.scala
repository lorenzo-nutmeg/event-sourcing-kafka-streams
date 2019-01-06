package org.amitayh.invoices.commandhandler

import java.time.LocalDate.now
import java.util.UUID
import java.util.UUID.randomUUID

import org.amitayh.invoices.commandhandler.CommandHandlerSpec._
import org.amitayh.invoices.common.Config
import org.amitayh.invoices.common.domain.Command._
import org.amitayh.invoices.common.domain.Event.InvoiceCreated
import org.amitayh.invoices.common.domain._
import org.amitayh.invoices.common.serde.AvroSerde.{CommandResultSerde, CommandSerde, EventSerde, SnapshotSerde}
import org.amitayh.invoices.common.serde.{UuidDeserializer, UuidSerializer}
import org.apache.kafka.clients.producer.ProducerRecord
import org.scalatest.{FunSpec, Matchers, OptionValues}


class CommandHandlerSpec extends FunSpec
  with CommandHandlerTopologyDefinition with KafkaStreamTopologyTest
  with Matchers with OptionValues {

  describe("The Command Handler") {
    describe("when receiving a Create Invoice command") {

      val originId = randomUUID
      val command = aCreateInvoiceCommand(originId)

      it("should produce a single Invoice Created event with version=1") {

        testTopology(() => {
          val commandTopic = createInputTopic[UUID, Command](Config.Topics.Commands.name, UuidSerializer, CommandSerde.serializer)
          val eventTopic: OutputTopic[UUID, Event] = createOutputTopic[UUID, Event](Config.Topics.Events.name, UuidDeserializer, EventSerde.deserializer)

          commandTopic.pipeIn(randomUUID, command)

          val firstRecord = eventTopic.receive
          firstRecord should be('defined)


          val event: Event = firstRecord
          event.commandId should be(command.commandId)
          event.version should be(1)
          event.payload shouldBe a[InvoiceCreated]


          assert(eventTopic.receive.isEmpty, "No more Events expected")
        })
      }


      it("should produce a single successful Command Result") {
        testTopology(()=>{
          val commandTopic = createInputTopic[UUID, Command](Config.Topics.Commands.name, UuidSerializer, CommandSerde.serializer)
          val commandResultTopic = createOutputTopic[UUID, CommandResult](Config.Topics.CommandResults.name, UuidDeserializer, CommandResultSerde.deserializer)

          commandTopic.pipeIn(randomUUID, command)

          val firstRecord = commandResultTopic.receive

          firstRecord should be('defined)

          val commandResult: CommandResult = firstRecord
          commandResult.originId should be (originId)
          commandResult.outcome shouldBe a[CommandResult.Success]

          assert(commandResultTopic.receive isEmpty, "No more CommandResults expected")
        })
      }

      it("should produce a single Invoice Snapshot with status 'New' and version=1") {
        testTopology(() => {
          val commandTopic = createInputTopic[UUID, Command](Config.Topics.Commands.name, UuidSerializer, CommandSerde.serializer)
          val invoiceSnapshotTopic = createOutputTopic[UUID,InvoiceSnapshot](Config.Topics.Snapshots.name, UuidDeserializer, SnapshotSerde.deserializer)

          commandTopic.pipeIn(randomUUID, command)

          val firstRecord = invoiceSnapshotTopic.receive

          firstRecord should be('defined)

          val invoiceSnapshot: InvoiceSnapshot = firstRecord
          invoiceSnapshot.version should be (1)
          invoiceSnapshot.invoice.status should be (InvoiceStatus.New)

          assert(invoiceSnapshotTopic.receive isEmpty, "No more InvoiceSnapshots expected")
        })
      }
    }
  }

}

object CommandHandlerSpec {
  implicit def optionProducerRecordToEvent(maybeRecord: Option[ProducerRecord[UUID,Event]]): Event =
    maybeRecord.get.value

  implicit def optionProducerRecordToCommandResult(maybeRecord: Option[ProducerRecord[UUID, CommandResult]]): CommandResult =
    maybeRecord.get.value

  implicit def optionProducerRecordToInvoiceSnapshot(maybeRecord: Option[ProducerRecord[UUID, InvoiceSnapshot]]): InvoiceSnapshot =
    maybeRecord.get.value

  def aCreateInvoiceCommand(originId: UUID): Command = Command(
    originId = originId,
    commandId = randomUUID,
    expectedVersion = None,
    CreateInvoice("a-customer", "a.customer@ema.il", now, now plusDays 1, List())
  )

}
