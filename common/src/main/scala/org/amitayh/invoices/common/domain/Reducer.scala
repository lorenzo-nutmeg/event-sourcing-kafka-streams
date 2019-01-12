package org.amitayh.invoices.common.domain

trait Reducer[S, E] {
  def empty: S
  def handle(s: S, e: E): S
}

/**
  * Invoice Event handler.
  * It applies different type of Event to update the state of the Invoice
  * It also defines the initial state of a pristine Invoice.
  */
object InvoiceReducer extends Reducer[Invoice, Event.Payload] {

  // This implicitly defines the initial state of the Invoice
  // FIXME this is not quite right: it works for CreateInvoice, but makes other commands not to fail when the invoice does not exist
  override val empty: Invoice = Invoice.None

  override def handle(invoice: Invoice, event: Event.Payload): Invoice = event match {
      // Here is the business logic defining the aggregate state changes when an event occurs.
      // FIXME It looks inconsistent with the implementation of Command:
      //       for Commands the business logic is in the `apply` method of Command.Payload subtype (no pattern matching)
      //       for Events, the business logic is centralised in a single method using pattern matching
    case Event.InvoiceCreated(customerName, customerEmail, issueDate, dueDate) =>
      invoice
        .setCustomer(customerName, customerEmail)
        .setDates(issueDate, dueDate)

    case Event.LineItemAdded(description, quantity, price) =>
      invoice.addLineItem(description, quantity, price)

    case Event.LineItemRemoved(index) =>
      invoice.removeLineItem(index)

    case Event.PaymentReceived(amount) =>
      invoice.pay(amount)

    case Event.InvoiceDeleted() =>
      invoice.delete

    case _ => invoice
  }
}

/**
  * Applies an Event to a snapshot
  * It actually relies on the InvoiceReducer to update the Invoice state, but additionally stores the event timestamp and version.
  *
  * It also checks the version of the event corresponds to the snapshot it is going to be applied to
  * (precisely, the Event version is the Snapshot version + 1)
  */
object SnapshotReducer extends Reducer[InvoiceSnapshot, Event] {
  override val empty: InvoiceSnapshot = EmptyInvoiceSnapshot

  override def handle(snapshot: InvoiceSnapshot, event: Event): InvoiceSnapshot = {
    if (versionsMatch(snapshot, event)) updateSnapshot(snapshot, event)
    else throw new RuntimeException(s"Unexpected version $snapshot / $event")
  }

  private def versionsMatch(snapshot: InvoiceSnapshot, event: Event): Boolean =
    snapshot.version == (event.version - 1)

  private def updateSnapshot(snapshot: InvoiceSnapshot, event: Event): InvoiceSnapshot = {
    val invoice = InvoiceReducer.handle(snapshot.invoice, event.payload)
    InvoiceSnapshot(invoice, event.version, event.timestamp)
  }
}
