package org.amitayh.invoices.commandhandler

import java.time.LocalDate.now
import java.util.UUID
import java.util.UUID.randomUUID

import org.amitayh.invoices.commandhandler.CommandHandlerSpec._
import org.amitayh.invoices.common.Config
import org.amitayh.invoices.common.domain.Command._
import org.amitayh.invoices.common.domain.Event.InvoiceCreated
import org.amitayh.invoices.common.domain.{Command, Event}
import org.amitayh.invoices.common.serde.AvroSerde.{CommandSerde, EventSerde}
import org.amitayh.invoices.common.serde.{UuidDeserializer, UuidSerializer}
import org.apache.kafka.clients.producer.ProducerRecord
import org.scalatest.{FunSpec, Matchers, OptionValues}

class CommandHandlerSpec extends FunSpec
  with CommandHandlerTopologyDefinition with KafkaStreamTopologyTest
  with Matchers with OptionValues {

  describe("The Command Handler") {
    describe("when receiving a Create Invoice command") {
      it("produces an Invoice Created event") {

        testTopology(() => {
          val commandTopic = createInputTopic[UUID, Command](Config.Topics.Commands.name, UuidSerializer, CommandSerde.serializer)
          val eventTopic = createOutputTopic[UUID, Event](Config.Topics.Events.name, UuidDeserializer, EventSerde.deserializer)

          val aCreateInvoiceCommand: Command = Command(
            originId = randomUUID,
            commandId = randomUUID,
            expectedVersion = None,
            CreateInvoice("a-customer", "a.customer@ema.il", now, now plusDays 1, List())
          )


          commandTopic.pipeIn(randomUUID, aCreateInvoiceCommand)

          val firstRecord = eventTopic.pipeOut
          val anotherRecord = eventTopic.pipeOut

          firstRecord should be('defined)
          anotherRecord should not be ('defined)

          val event: Event = firstRecord
          event.commandId should be(aCreateInvoiceCommand.commandId)
          event.version should be(1)
          event.payload shouldBe a[InvoiceCreated]
        })
      }
    }
  }

}

object CommandHandlerSpec {
  implicit def optionProducerRecordToEvent(maybeRecord: Option[ProducerRecord[UUID,Event]]): Event =
    maybeRecord.get.value
}
