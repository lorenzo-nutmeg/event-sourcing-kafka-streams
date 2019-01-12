package org.amitayh.invoices.common.domain

import java.time.{Instant, LocalDate}
import java.util.UUID

import org.amitayh.invoices.common.domain.Command.failure

import scala.collection.immutable.Seq

case class Command(originId: UUID,
                   commandId: UUID,
                   expectedVersion: Option[Int],
                   payload: Command.Payload) {

  def apply(timestamp: Instant, snapshot: InvoiceSnapshot): CommandResult = {
    val outcome = snapshot
      .validateVersion(expectedVersion) // Validate whether the expected aggregate version matches the actual version of the snapshot
      .flatMap(payload(_)) //...applies the actual command payload, returning a Command.Result (i.e. Either a sequence of Event.Payloads or an error)
      .fold(
        CommandResult.Failure, //... on error (left), returns a CommandResult.Failure
        success(timestamp, snapshot, _)) //... on success (right), applies all Event.Payloads to the original snapshot

    CommandResult(originId, commandId, outcome)
  }

  private def success(timestamp: Instant,
                      snapshot: InvoiceSnapshot,
                      payloads: Seq[Event.Payload]): CommandResult.Outcome = {
    payloads.foldLeft(CommandResult.Success(snapshot)) { (acc, payload) =>
      acc.update(timestamp, commandId, payload)
    }
  }

}

object Command {
  type Result = Either[InvoiceError, Seq[Event.Payload]]

  // Different Command.Payload types define the business logic of the command handler:
  // - Is the Command valid, given the current state of the aggregate?
  // - What Event(s) should be emitted, as effect of the Command?
  //
  // Note that the aggregate state changes are NOT defined here, but in the InvoiceReducer

  sealed trait Payload {
    def apply(invoice: Invoice): Result
  }

  case class CreateInvoice(customerName: String,
                           customerEmail: String,
                           issueDate: LocalDate,
                           dueDate: LocalDate,
                           lineItems: List[LineItem]) extends Payload {

    override def apply(invoice: Invoice): Result = {
      invoice match {
        case Invoice.None => {
          val createdEvent = Event.InvoiceCreated(customerName, customerEmail, issueDate, dueDate)
          val lineItemEvents = lineItems.map(toLineItemEvent)
          success(createdEvent :: lineItemEvents)
        }
        case _ => failure(InvoiceAlreadyExists())
      }
    }

    private def toLineItemEvent(lineItem: LineItem): Event.Payload =
      Event.LineItemAdded(
        description = lineItem.description,
        quantity = lineItem.quantity,
        price = lineItem.price)
  }

  case class AddLineItem(description: String,
                         quantity: Double,
                         price: Double) extends Payload {
    override def apply(invoice: Invoice): Result = invoice match {
      case Invoice.None => failure(InvoiceDoesNotExist())
      case _ => success(Event.LineItemAdded(description, quantity, price))

    }
  }

  case class RemoveLineItem(index: Int) extends Payload {
    override def apply(invoice: Invoice): Result = invoice match {
      case Invoice.None => failure(InvoiceDoesNotExist())
      case _  if (invoice.hasLineItem(index)) => success(Event.LineItemRemoved(index))
      case _ => failure(LineItemDoesNotExist(index))
    }

  }

  case class PayInvoice() extends Payload {
    // FIXME It should fail if the invoice is already paid
    override def apply(invoice: Invoice): Result = invoice match {
      case Invoice.None => failure(InvoiceDoesNotExist())
      case _ => success(Event.PaymentReceived(invoice.total))
    }
  }

  case class DeleteInvoice() extends Payload {
    override def apply(invoice: Invoice): Result = invoice match {
      case Invoice.None => failure(InvoiceDoesNotExist())
      case _ => success(Event.InvoiceDeleted())
    }

  }

  private def success(events: Event.Payload*): Result = success(events.toList)

  private def success(events: List[Event.Payload]): Result = Right(events)

  private def failure(error: InvoiceError): Result = Left(error)
}
