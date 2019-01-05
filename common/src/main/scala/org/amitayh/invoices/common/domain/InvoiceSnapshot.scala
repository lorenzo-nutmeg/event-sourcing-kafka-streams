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
