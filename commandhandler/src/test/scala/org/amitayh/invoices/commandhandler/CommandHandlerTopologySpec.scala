package org.amitayh.invoices.commandhandler

import java.time.Instant
import java.time.LocalDate.now
import java.util.UUID.randomUUID
import java.util.{Properties, UUID}

import org.amitayh.invoices.commandhandler.CommandHandlerTopologySpec._
import org.amitayh.invoices.common.Config
import org.amitayh.invoices.common.domain.Command._
import org.amitayh.invoices.common.domain.Event.{apply => _, _}
import org.amitayh.invoices.common.domain._
import org.amitayh.invoices.common.serde.AvroSerde.{CommandResultSerde, CommandSerde, EventSerde, SnapshotSerde}
import org.amitayh.invoices.common.serde.{UuidDeserializer, UuidSerializer}
import org.amitayh.invoices.streamprocessor.TopologyTest
import org.apache.kafka.streams.{StreamsConfig, TopologyTestDriver => Driver}
import org.scalatest.{FunSpec, Matchers, OptionValues}

class CommandHandlerTopologySpec extends FunSpec with TopologyTest with Matchers with OptionValues{

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

        it("should emit a failed CommandResult and DO NOT emit any event or snapshot and do not change the state store") {
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

        it("should emit a failed CommandResult and DO NOT emit any event or snapshot") {
          test(exactlyOnce)((driver) => {
            implicit val d = driver

            commandTopic.pipeIn(invoiceId, command)

            assertNextCommandResultIsFailure(commandResultTopic)

            assertNoMoreMessages(commandResultTopic, eventTopic, snapshotTopic)
          })
        }
      }

      describe("with the wrong expected version") {
        val initialVersion = 1
        val initialInvoiceSnapshot = anEmptyInvoiceSnapshot(initialVersion, InvoiceStatus.New)
        val invoiceId = randomUUID
        val command = aDeleteInvoiceCommand(Option(42))

        it("should emit a failed CommandResult and DO NOT emit any event or snapshot, and do not update the state store") {
          test(exactlyOnce)((driver) => {
            implicit val d = driver

            setSnapshotState(snapshotStore)(invoiceId, initialInvoiceSnapshot)

            commandTopic.pipeIn(invoiceId, command)

            assertNextCommandResultIsFailure(commandResultTopic)
            assertSnapshotStateInvoiceStatusAndLineItemsAndVersion(snapshotStore)(invoiceId)(InvoiceStatus.New, Vector(), initialVersion)

            assertNoMoreMessages(commandResultTopic, eventTopic, snapshotTopic)
          })
        }
      }

    }

    describe("when receiving an AddLineItem command") {
      val initialVersion = 1
      val initialInvoiceSnapshot = anEmptyInvoiceSnapshot(initialVersion, InvoiceStatus.New)
      val invoiceId = randomUUID

      val (newLineDescription, newLineQuantity, newLinePrice) = ("foo-bar", 13.0, 42)
      val command = anAddLineItemCommand(newLineDescription, newLineQuantity, newLinePrice, Option(initialVersion))

      describe("on an existing empty invoice") {
        it("should emit a successful CommandResult, an InvoiceSnapshot and an LineItemAdded event, should update the state store") {
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

    describe("when receiving a DeleteLineItem command") {
      val initialVersion = 42
      val initialLineItems = Vector( LineItem("first item", 1.0, 12.3), LineItem("second item", 2.5, 11.2), LineItem("third item", 42.3, 12.34))
      val initialInvoiceSnapshot = anInvoiceSnapshot(initialVersion, InvoiceStatus.New, initialLineItems)
      val invoiceId = randomUUID

      describe("for an existing item") {
        val indexToRemove = 1
        val deleteItemCommand = aRemoveItemCommand(Option(initialVersion), indexToRemove)

        it("should emit a successful CommandResult, an InvoiceSnapshot and a LineItemRemoved event, should update the state store") {
          test(exactlyOnce)((driver) => {
            implicit val d = driver

            setSnapshotState(snapshotStore)(invoiceId, initialInvoiceSnapshot)

            commandTopic.pipeIn(invoiceId, deleteItemCommand)

            assertNextCommandResultIsSuccess(commandResultTopic)
            assertNextEventIsEventTypeAndVersionAndCommandId(eventTopic)(classOf[LineItemRemoved], initialVersion + 1, deleteItemCommand.commandId)

            val expectedLineItems = initialLineItems filterNot ( _ == initialLineItems(indexToRemove))
            assertNextSnapshotHasInvoiceStatusAndLineItemsAndVersion(snapshotTopic)(InvoiceStatus.New, expectedLineItems, initialVersion + 1)
            assertSnapshotStateInvoiceStatusAndLineItemsAndVersion(snapshotStore)(invoiceId)(InvoiceStatus.New, expectedLineItems, initialVersion+1 )

            assertNoMoreMessages(commandResultTopic, eventTopic, snapshotTopic)
          })
        }
      }

      describe("for a non existing item") {
        val indexToRemove = 99
        val deleteItemCommand = aRemoveItemCommand(Option(initialVersion), indexToRemove)

        it("should emit a failed CommandResult and DO NOT emit any event or snapshot, and do not update the state store") {
          test(exactlyOnce)((driver) => {
            implicit val d = driver

            setSnapshotState(snapshotStore)(invoiceId, initialInvoiceSnapshot)

            commandTopic.pipeIn(invoiceId, deleteItemCommand)

            assertNextCommandResultIsFailure(commandResultTopic)
            assertSnapshotStateInvoiceStatusAndLineItemsAndVersion(snapshotStore)(invoiceId)(InvoiceStatus.New, initialLineItems, initialVersion )

            assertNoMoreMessages(commandResultTopic, eventTopic, snapshotTopic)
          })
        }
      }

    }

    describe("when receiving a PayInvoice command") {
      val initialVersion = 1
      val initialInvoiceSnapshot = anEmptyInvoiceSnapshot(initialVersion, InvoiceStatus.New)
      val invoiceId = randomUUID
      val payInvoiceCommand = aPayInvoiceCommand(Option(initialVersion))

      it("should emit a successful CommandResult, an InvoiceSnapshot and a PaymentReceived event, should update the state store") {
        test(exactlyOnce)((driver) => {
          implicit val d = driver

          setSnapshotState(snapshotStore)(invoiceId, initialInvoiceSnapshot)

          commandTopic.pipeIn(invoiceId, payInvoiceCommand)

          assertNextCommandResultIsSuccess(commandResultTopic)
          assertNextEventIsEventTypeAndVersionAndCommandId(eventTopic)(classOf[PaymentReceived], initialVersion + 1, payInvoiceCommand.commandId)

          val expectedStatus = InvoiceStatus.Paid
          assertNextSnapshotHasInvoiceStatusAndLineItemsAndVersion(snapshotTopic)(expectedStatus, Vector(), initialVersion + 1)
          assertSnapshotStateInvoiceStatusAndLineItemsAndVersion(snapshotStore)(invoiceId)(expectedStatus, Vector(), initialVersion+1 )

          assertNoMoreMessages(commandResultTopic, eventTopic, snapshotTopic)
        })
      }
    }

    // ... might add more corner cases
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


object CommandHandlerTopologySpec {

  val exactlyOnce = {
    val props = new Properties
    props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE)
    props
  }

  def aCommand(expVersion: Option[Int], payload: Command.Payload): Command = Command(randomUUID, randomUUID, expVersion, payload)
  def aCreateInvoiceCommand: Command = aCommand(None, CreateInvoice("a-customer", "a.customer@ema.il", now, now plusDays 1, List()))
  def aDeleteInvoiceCommand(expVersion: Option[Int]): Command = aCommand(expVersion, DeleteInvoice())
  def anAddLineItemCommand(newLineDescription: String, newLineQuantity: Double, newLinePrice: Double, expVersion: Option[Int]): Command = aCommand(expVersion,  AddLineItem(newLineDescription, newLineQuantity, newLinePrice))
  def aRemoveItemCommand(expVersion: Option[Int], index: Int) = aCommand(expVersion, RemoveLineItem(index))
  def aPayInvoiceCommand(expVersion: Option[Int]) = aCommand(expVersion, PayInvoice())

  def anInvoiceSnapshot(version: Int, status: InvoiceStatus, lineItems: Vector[LineItem]) = InvoiceSnapshot(
    Invoice(
      Customer("a-customer", "a.customer@ema.il"),
      now,
      now plusDays 1,
      lineItems,
      status,
      0.00
    ),
    version,
    Instant.now
  )
  def anEmptyInvoiceSnapshot(version: Int, status: InvoiceStatus): InvoiceSnapshot = anInvoiceSnapshot(version, status, Vector())

}