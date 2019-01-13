package org.amitayh.invoices.common.serde

import java.time.{Instant, LocalDate}
import java.util.UUID
import java.util.UUID.randomUUID

import org.amitayh.invoices.common.domain.Command.{apply => _, _}
import org.amitayh.invoices.common.domain.Event._
import org.amitayh.invoices.common.domain._
import org.amitayh.invoices.common.serde.AvroSerdeSpec._
import org.apache.kafka.common.serialization.Serde
import org.scalatest.FlatSpec

class AvroSerdeSpec extends FlatSpec with SerdeBehavious {

  // ...maybe there is a better way for not repeating the message


  val commndResultSerde: Serde[CommandResult] = AvroSerde.CommandResultSerde
  "A CommandResult Serde with a successful CommandResult" should behave like roundtripSerDe(commndResultSerde, aSuccessfulCommandResult)
  "A CommandResult Serde with a failed CommandResult containing a VersionMismatch" should behave like roundtripSerDe(commndResultSerde, aFailedCommandResult(VersionMismatch(Option(42), Option(13))))
  "A CommandResult Serde with a failed CommandResult containing a LineItemDoesNotExist" should behave like roundtripSerDe(commndResultSerde, aFailedCommandResult(LineItemDoesNotExist(42)))
  "A CommandResult Serde with a failed CommandResult containing a InvoiceAlreadyExists" should behave like roundtripSerDe(commndResultSerde, aFailedCommandResult(InvoiceAlreadyExists()))
  "A CommandResult Serde with a failed CommandResult containing a InvoiceDoesNotExist" should behave like roundtripSerDe(commndResultSerde, aFailedCommandResult(InvoiceDoesNotExist()))



  val eventSerde: Serde[Event] = AvroSerde.EventSerde
  "An Event Serde with an InvoiceCreated event" should behave like roundtripSerDe(eventSerde, anInvoiceCreatedEvent)
  "An Event Serde with an InvoiceDeleted event" should behave like roundtripSerDe(eventSerde, anInvoiceDeletedEvent)
  "An Event Serde with a LineItemAdded event" should behave like roundtripSerDe(eventSerde, aLineItemAddedEvent)
  "An Event Serde with a LineItemRemoved event" should behave like roundtripSerDe(eventSerde, aLineItemRemovedEvent)
  "An Event Serde with a PaymentReceived event" should behave like roundtripSerDe(eventSerde, aPaymentReceivedEvent)


  val snapshotSerde: Serde[InvoiceSnapshot] = AvroSerde.SnapshotSerde
  "An InvoiceSnapshot Serde with an empty, new InvoiceSnapshot" should behave like roundtripSerDe(snapshotSerde, anEmptyNewInvoiceSnapshot)
  "An InvoiceSnapshot Serde with a two-items, new InvoiceSnapshot" should behave like roundtripSerDe(snapshotSerde, anInvoiceSnapshotWithTwoLineItems)


  val commandSerde: Serde[Command] = AvroSerde.CommandSerde
  "A Command Serde with a CreateInvoice command" should behave like roundtripSerDe(commandSerde, aCreateInvoiceCommand)
  "A Command Serde with a DeleteInvoice command" should behave like roundtripSerDe(commandSerde, aDeleteInvoiceCommand)
  "A Command Serde with a AddLineItem command" should behave like roundtripSerDe(commandSerde, anAddLineItemCommand)
  "A Command Serde with a RemoveLineItem command" should behave like roundtripSerDe(commandSerde, aRemoveLineItemCommand)
  "A Command Serde with a PayInvoice command" should behave like roundtripSerDe(commandSerde, aPayInvoiceCommand)

}



object AvroSerdeSpec {


  def anInvoiceCreatedEvent = Event(1, Instant.now, randomUUID, InvoiceCreated("a-customer", "a.customer@ema.il", LocalDate.now, LocalDate.now plusDays 1))
  def aLineItemAddedEvent = Event(43, Instant.now, randomUUID, LineItemAdded("idem description", 43.0, 12.34))
  def aLineItemRemovedEvent = Event(44, Instant.now, randomUUID, LineItemRemoved(7))
  def aPaymentReceivedEvent = Event(44, Instant.now, randomUUID, PaymentReceived(12.34))
  def anInvoiceDeletedEvent: Event = anInvoiceDeletedEvent(randomUUID)
  def anInvoiceDeletedEvent(commandId: UUID) = Event(42, Instant.now, commandId, InvoiceDeleted())


  def anEmptyNewInvoiceSnapshot: InvoiceSnapshot = anEmptyNewInvoiceSnapshot(1)

  def anEmptyNewInvoiceSnapshot(version: Int): InvoiceSnapshot = InvoiceSnapshot(
    Invoice(
      Customer("a-customer", "a.customer@ema.il"),
      LocalDate.now,
      LocalDate.now plusDays 1,
      Vector(),
      InvoiceStatus.New,
      0.00
    ),
    version,
    Instant.now
  )

  def followingDeletedInvoiceSnapshot(prev: InvoiceSnapshot): InvoiceSnapshot = InvoiceSnapshot(
    prev.invoice,
    prev.version + 1,
    Instant.now
  )

  def anInvoiceSnapshotWithTwoLineItems: InvoiceSnapshot = InvoiceSnapshot(
    Invoice(
      Customer("a-customer", "a.customer@ema.il"),
      LocalDate.now,
      LocalDate.now plusDays 1,
      Vector( LineItem("an item", 1.0, 11.0), LineItem("another item", 2.0, 12.0)),
      InvoiceStatus.New,
      0.00
    ),
    1,
    Instant.now
  )

  def aSuccessfulCommandResult: CommandResult = {
    val commandId = randomUUID
    val event = anInvoiceDeletedEvent(commandId)
    val oldSnapshot = anEmptyNewInvoiceSnapshot(42)
    val newSnapshot = followingDeletedInvoiceSnapshot(oldSnapshot)
    CommandResult(
      randomUUID,
      commandId,
      CommandResult.Success(
        Vector(event),
        oldSnapshot,
        newSnapshot
      )
    )
  }


  def aFailedCommandResult(error: InvoiceError): CommandResult = {
    val commandId = randomUUID
    CommandResult(
      randomUUID,
      commandId,
      CommandResult.Failure(error)
    )
  }

  def aCommandWith(payload: Command.Payload) = Command( randomUUID, randomUUID, Option(42), payload)


  def aCreateInvoiceCommand = aCommandWith(CreateInvoice("a-customer", "a.customer@ema.il", LocalDate.now, LocalDate.now plusDays 1, List()))

  def aDeleteInvoiceCommand = aCommandWith(DeleteInvoice())

  def anAddLineItemCommand = aCommandWith(AddLineItem("item-description", 42.0, 12.23))

  def aRemoveLineItemCommand = aCommandWith(RemoveLineItem(3))

  def aPayInvoiceCommand = aCommandWith(PayInvoice())
}

