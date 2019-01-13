package org.amitayh.invoices.common.domain

import java.time.Instant

import org.scalatest.{EitherValues, FunSpec, Matchers}

// A not really useful test to start with...
class InvoiceSnapshotSpec extends FunSpec with Matchers with EitherValues {

  describe("An Invoice Snapshot") {
    describe ("on validating the expected version") {
      val snapshotVersion = 42
      val anInvoiceSnapshot = InvoiceSnapshot(anInvoice, snapshotVersion, Instant.now)

      def validateVersion(expectedVersion: Option[Int]) = anInvoiceSnapshot.validateVersion(expectedVersion)

      describe( "when expected version matches snapshot version") {
        val expectedVersion = Some(42)

        it ("should return the invoice") {
          validateVersion(expectedVersion).right.value should be (anInvoice)
        }
      }

      describe("when no expected version") {
        val expectedVersion = None

        it("should return the invoice") {
          validateVersion(expectedVersion).right.value should be (anInvoice)
        }
      }

      describe("when expected version DOES NOT match snapshot version") {
        val expectedVersion = Some(13)

        it("should report a version mismatch") {
          validateVersion(expectedVersion).left.value should be (VersionMismatch(Option(snapshotVersion), expectedVersion))
        }
      }
    }
  }
}

