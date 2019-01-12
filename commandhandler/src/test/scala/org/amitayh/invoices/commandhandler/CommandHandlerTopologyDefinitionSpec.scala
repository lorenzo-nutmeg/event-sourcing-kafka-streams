package org.amitayh.invoices.commandhandler

import java.time.Instant
import java.time.LocalDate.now
import java.util.UUID.randomUUID
import java.util.{Properties, UUID}

import org.amitayh.invoices.commandhandler.CommandHandlerTopologyDefinitionSpec._
import org.amitayh.invoices.common.Config
import org.amitayh.invoices.common.domain.Command.{AddLineItem, CreateInvoice, DeleteInvoice}
import org.amitayh.invoices.common.domain.Event.{InvoiceCreated, InvoiceDeleted, LineItemAdded}
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

      describe("for a non existing invoice") {
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
            assertNextSnapshotHasInvoiceStatusAndLineItemsAndVersion(snapshotTopic)(InvoiceStatus.New, List(), 1)
            assertSnapshotStateInvoiceStatusAndLineItemsAndVersion(snapshotStore)(invoiceId)(InvoiceStatus.New, List(), 1 )


            assertNoMoreMessages(commandResultTopic, eventTopic, snapshotTopic)
          })
        }
      }

      describe("for an existing invoice") {
        val initialVersion = 1
        val initialInvoiceSnapshot = anEmptyInvoiceSnapshot(initialVersion, InvoiceStatus.New)
        val invoiceId = randomUUID
        val command = aCreateInvoiceCommand

        ignore("should emit a Failure CommandResult and DO NOT emit any event or snapshot and do not change the state of the existing invoice") {
          test(exactlyOnce)((driver) => {
            implicit val d = driver

            setSnapshotState(snapshotStore)(invoiceId, initialInvoiceSnapshot)

            commandTopic.pipeIn(invoiceId, command)

            assertNextCommandResultIsFailure(commandResultTopic)
            assertSnapshotStateInvoiceStatusAndLineItemsAndVersion(snapshotStore)(invoiceId)(InvoiceStatus.New, List(), initialVersion )

            assertNoMoreMessages(commandResultTopic, eventTopic, snapshotTopic)
          })
        }

      }

    }

    describe("when receiving a DeleteInvoice command") {


      describe("on an existing invoice") {
        val initialVersion = 1
        val initialInvoiceSnapshot = anEmptyInvoiceSnapshot(initialVersion, InvoiceStatus.New)
        val invoiceId = randomUUID
        val command = aDeleteInvoiceCommand(Option(initialVersion))

        it("should emit an InvoiceSnapshot, a successful CommandResult and an InvoiceDeleted event with status 'Deleted' and version incremented by 1, should update the state store with an InvoiceSnapshot in status=Deleted and version incremented by 1") {

          test(exactlyOnce)((driver) => {
            implicit val d = driver

            setSnapshotState(snapshotStore)(invoiceId, initialInvoiceSnapshot)

            commandTopic.pipeIn(invoiceId, command)

            assertNextCommandResultIsSuccess(commandResultTopic)
            assertNextEventIsEventTypeAndVersionAndCommandId(eventTopic)(classOf[InvoiceDeleted], initialVersion + 1, command.commandId)
            assertNextSnapshotHasInvoiceStatusAndLineItemsAndVersion(snapshotTopic)(InvoiceStatus.Deleted, List(), initialVersion + 1)
            assertSnapshotStateInvoiceStatusAndLineItemsAndVersion(snapshotStore)(invoiceId)(InvoiceStatus.Deleted, List(), initialVersion+1 )

            assertNoMoreMessages(commandResultTopic, eventTopic, snapshotTopic)
          })
        }
      }

      describe("on a not existing invoice") {
        val invoiceId = randomUUID
        val command = aDeleteInvoiceCommand(None)

        ignore("should emit a Failure CommandResult and DO NOT emit any event or snapshot") {
          test(exactlyOnce)((driver) => {
            implicit val d = driver

            commandTopic.pipeIn(invoiceId, command)

            assertNextCommandResultIsFailure(commandResultTopic)

            assertNoMoreMessages(commandResultTopic, eventTopic, snapshotTopic)
          })
        }
      }


      // TODO Test the behaviour when the version does not match
    }

    describe("when receiving an AddLineItem command") {
      val initialVersion = 1
      val initialInvoiceSnapshot = anEmptyInvoiceSnapshot(initialVersion, InvoiceStatus.New)
      val invoiceId = randomUUID

      val (newLineDescription, newLineQuantity, newLinePrice) = ("foo-bar", 13.0, 42)
      val command = anAddLineItemCommand(newLineDescription, newLineQuantity, newLinePrice, Option(initialVersion))

      describe("on an existing empty invoice") {
        it("should emit a successful CommandResult, an InvoiceSnapshot and an LineItemAdded event matching the added line, should update the state store") {
          test(exactlyOnce)((driver) => {
            implicit val d = driver

            setSnapshotState(snapshotStore)(invoiceId, initialInvoiceSnapshot)

            commandTopic.pipeIn(invoiceId, command)

            assertNextCommandResultIsSuccess(commandResultTopic)
            assertNextEventIsEventTypeAndVersionAndCommandId(eventTopic)(classOf[LineItemAdded], initialVersion + 1, command.commandId)

            val expectedLineItems = List(LineItem(newLineDescription, newLineQuantity, newLinePrice))
            assertNextSnapshotHasInvoiceStatusAndLineItemsAndVersion(snapshotTopic)(InvoiceStatus.New, expectedLineItems, initialVersion + 1)
            assertSnapshotStateInvoiceStatusAndLineItemsAndVersion(snapshotStore)(invoiceId)(InvoiceStatus.New, expectedLineItems, initialVersion+1 )

            assertNoMoreMessages(commandResultTopic, eventTopic, snapshotTopic)
          })
        }
      }
    }

    // TODO Test removing a non-existing line item

    // TODO Test other commands
  }

  def setSnapshotState(store: KVStore[UUID,InvoiceSnapshot])(invoiceId: UUID, state: InvoiceSnapshot )(implicit driver: () => Driver): KVStore[UUID,InvoiceSnapshot] = {
    store.put(invoiceId, state)
    store
  }

  def assertNextCommandResultIsSuccess(topic: TopologyTest#OutputTopic[UUID,CommandResult])(implicit driver: () => Driver) = {
    val maybeRecord = topic.popOut
    val commandResult: CommandResult = maybeRecord.value.value
    commandResult.outcome shouldBe a[CommandResult.Success]
  }

  def assertNextCommandResultIsFailure(topic: TopologyTest#OutputTopic[UUID,CommandResult])(implicit driver: () => Driver) = {
    val maybeRecord = topic.popOut
    val commandResult: CommandResult = maybeRecord.value.value
    commandResult.outcome shouldBe a[CommandResult.Failure]
  }

  def assertSnapshotStateInvoiceStatusAndLineItemsAndVersion(store: KVStore[UUID,InvoiceSnapshot])(invoiceId: UUID)(expectedInvoiceStatus: InvoiceStatus, expectedLineItems: Seq[LineItem], expectedVersion: Int  )(implicit driver: () => Driver) = {
    val snapshot = store.get(invoiceId)
    snapshot.value.invoice.status should be (expectedInvoiceStatus)
    snapshot.value.version should be (expectedVersion)

    snapshot.value.invoice.lineItems should contain theSameElementsInOrderAs expectedLineItems
  }

  def assertSnapshotStateDoesNotExist(store: KVStore[UUID,InvoiceSnapshot])(invoiceId: UUID)(implicit driver: () => Driver) = {
    val snapshotStateBefore = store.get(invoiceId)
    snapshotStateBefore should not be ('defined)
  }


  def assertNextEventIsEventTypeAndVersionAndCommandId(topic: TopologyTest#OutputTopic[UUID,Event])(expectedEventType: Class[_], expectedVersion: Int, expectedCommandId: UUID )(implicit driver: () => Driver) = {
    val eventRecord = topic.popOut
    val event: Event = eventRecord.value.value
    event.commandId should be(expectedCommandId)
    event.version should be(expectedVersion)
    assert( event.payload.getClass == expectedEventType)
  }


  def assertNextSnapshotHasInvoiceStatusAndLineItemsAndVersion(topic: TopologyTest#OutputTopic[UUID,InvoiceSnapshot])(expectedInvoiceStatus: InvoiceStatus, expectedLineItems: Seq[LineItem], expectedVersion: Int  )(implicit driver: () => Driver) = {
    val snapshotRecord = topic.popOut
    val snapshot: InvoiceSnapshot = snapshotRecord.value.value
    snapshot.version should be(expectedVersion)
    snapshot.invoice.status should be(expectedInvoiceStatus)

    snapshot.invoice.lineItems should contain theSameElementsInOrderAs expectedLineItems
  }

  def assertNoMoreMessages(topics: TopologyTest#OutputTopic[UUID,_]*)(implicit driver: () => Driver) = {
    topics.foreach( topic =>
      assert(topic.popOut isEmpty, "No more messages expected in '" + topic.topicName + "'")
    )
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



  def anAddLineItemCommand(newLineDescription: String, newLineQuantity: Double, newLinePrice: Double, expVersion: Option[Int]): Command = Command(
    originId = randomUUID,
    commandId = randomUUID,
    expectedVersion = expVersion,
    AddLineItem(newLineDescription, newLineQuantity, newLinePrice)
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