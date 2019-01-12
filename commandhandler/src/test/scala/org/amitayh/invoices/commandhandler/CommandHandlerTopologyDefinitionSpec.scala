package org.amitayh.invoices.commandhandler

import java.time.Instant
import java.time.LocalDate.now
import java.util.UUID.randomUUID
import java.util.{Properties, UUID}

import org.amitayh.invoices.commandhandler.CommandHandlerTopologyDefinitionSpec.{aCreateInvoiceCommand, aDeleteInvoiceCommand, anEmptyInvoiceSnapshot, _}
import org.amitayh.invoices.common.Config
import org.amitayh.invoices.common.domain.Command.{CreateInvoice, DeleteInvoice}
import org.amitayh.invoices.common.domain.Event.{InvoiceCreated, InvoiceDeleted}
import org.amitayh.invoices.common.domain._
import org.amitayh.invoices.common.serde.AvroSerde.{CommandResultSerde, CommandSerde, EventSerde, SnapshotSerde}
import org.amitayh.invoices.common.serde.{UuidDeserializer, UuidSerializer}
import org.amitayh.invoices.streamprocessor.TopologyTest
import org.apache.kafka.streams.{StreamsConfig, TopologyTestDriver => Driver}
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

      it("should emit a successful CommandResult, an InvoiceCreated event and an InvoiceSnapshot, with version=1 and InvoiceState=New, and should create a new Snapshot in the state store") {

        test(exactlyOnce)( driver => {
          implicit val d = driver

          // Record not there
          assertSnapshotStateDoesNotExist(snapshotStore)(invoiceId)

          commandTopic.pipeIn(invoiceId, command)

          assertNextCommandResultIsSuccess(commandResultTopic)
          assertNextEventIsEventTypeAndVersionAndCommandId(eventTopic)(classOf[InvoiceCreated], 1, command.commandId)
          assertNextSnapshotHasInvoiceStatusAndVersion(snapshotTopic)(InvoiceStatus.New, 1)
          assertSnapshotStateInvoiceStatusAndVersion(snapshotStore)(invoiceId)(InvoiceStatus.New, 1 )


          assertNoMoreMessages(commandResultTopic)
          assertNoMoreMessages(eventTopic)
          assertNoMoreMessages(eventTopic)
        })

      }
    }

    describe("when receiving a DeleteInvoice command") {

      val initialVersion = 1
      val initialInvoiceSnapshot = anEmptyInvoiceSnapshot(initialVersion, InvoiceStatus.New)
      val invoiceId = randomUUID
      val command = aDeleteInvoiceCommand(Option(initialVersion))

      describe("on an existing invoice") {
        it("should emit an InvoiceSnapshot, a successful CommandResult and an InvoiceDeleted event with status 'Deleted' and version incremented by 1, should update the state store with an InvoiceSnapshot in status=Deleted and version incremented by 1") {

          test(exactlyOnce)((driver) => {
            implicit val d = driver

            snapshotStore.put(invoiceId, initialInvoiceSnapshot)

            commandTopic.pipeIn(invoiceId, command)


            assertNextCommandResultIsSuccess(commandResultTopic)
            assertNextEventIsEventTypeAndVersionAndCommandId(eventTopic)(classOf[InvoiceDeleted], initialVersion + 1, command.commandId)
            assertNextSnapshotHasInvoiceStatusAndVersion(snapshotTopic)(InvoiceStatus.Deleted, initialVersion + 1)
            assertSnapshotStateInvoiceStatusAndVersion(snapshotStore)(invoiceId)(InvoiceStatus.Deleted, initialVersion+1 )

          })
        }
      }


      // TODO Test other events
    }
  }

  def assertSnapshotStateInvoiceStatusAndVersion(store: KVStore[UUID,InvoiceSnapshot])(invoiceId: UUID)(expectedInvoiceStatus: InvoiceStatus, expectedVersion: Int  )(implicit driver: () => Driver) = {
    val snapshotStateAfter = store.get(invoiceId)
    snapshotStateAfter.value.invoice.status should be (expectedInvoiceStatus)
    snapshotStateAfter.value.version should be (expectedVersion)
  }

  def assertSnapshotStateDoesNotExist(store: KVStore[UUID,InvoiceSnapshot])(invoiceId: UUID)(implicit driver: () => Driver) = {
    val snapshotStateBefore = store.get(invoiceId)
    snapshotStateBefore should not be ('defined)
  }

  def assertNextCommandResultIsSuccess(topic: TopologyTest#OutputTopic[UUID,CommandResult])(implicit driver: () => Driver) = {
    val maybeRecord = topic.popOut
    val commandResult: CommandResult = maybeRecord.value.value
    commandResult.outcome shouldBe a[CommandResult.Success]
  }


  def assertNextEventIsEventTypeAndVersionAndCommandId(topic: TopologyTest#OutputTopic[UUID,Event])(expectedEventType: Class[_], expectedVersion: Int, expectedCommandId: UUID )(implicit driver: () => Driver) = {
    val eventRecord = topic.popOut
    val event: Event = eventRecord.value.value
    event.commandId should be(expectedCommandId)
    event.version should be(expectedVersion)
    assert( event.payload.getClass == expectedEventType)

  }


  def assertNextSnapshotHasInvoiceStatusAndVersion(topic: TopologyTest#OutputTopic[UUID,InvoiceSnapshot])(expectedInvoiceStatus: InvoiceStatus, expectedVersion: Int  )(implicit driver: () => Driver) = {
    val snapshotRecord = topic.popOut
    val invoiceSnapshot: InvoiceSnapshot = snapshotRecord.value.value
    invoiceSnapshot.version should be(expectedVersion)
    invoiceSnapshot.invoice.status should be(expectedInvoiceStatus)
  }

  def assertNoMoreMessages(topic: TopologyTest#OutputTopic[UUID,_])(implicit driver: () => Driver) = {
    assert(topic.popOut isEmpty, "No more messages expected in '" + topic.topicName + "'")
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