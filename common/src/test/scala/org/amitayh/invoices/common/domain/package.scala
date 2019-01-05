package org.amitayh.invoices.common

import java.time.LocalDate.now

package object domain {
  val aCustomer = Customer ("customer-name", "customer-email")

  val anInvoice = Invoice (
    customer = aCustomer,
    issueDate = now,
    dueDate = now,
    lineItems = Vector(),
    status = InvoiceStatus.New,
    paid = null
  )
}
