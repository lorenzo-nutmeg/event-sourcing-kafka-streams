package org.amitayh.invoices.common.domain

import java.time.Instant

/**
  * Invoice Snapshot
  *
  * adds version and updated timestamp to the Invoice state
  */
case class InvoiceSnapshot(invoice: Invoice,
                           version: Int,
                           timestamp: Instant) {

  def validateVersion(expectedVersion: Option[Int]): Either[InvoiceError, Invoice] =
    if (expectedVersion.forall(_ == version)) Right(invoice)
    else Left(VersionMismatch(version, expectedVersion))

}

object EmptyInvoiceSnapshot extends InvoiceSnapshot(InvoiceReducer.empty, 0, Instant.MIN) {
  override def validateVersion(expectedVersion: Option[Int]): Either[InvoiceError, Invoice] =
    expectedVersion
        .fold[Either[InvoiceError, Invoice]](Right(Invoice.None))(_ => Left(VersionMismatch(0, expectedVersion)))

}