package org.amitayh.invoices.common.serde

import java.time.{Instant, LocalDate}
import java.util.UUID
import java.util.UUID.randomUUID

import org.amitayh.invoices.common.domain.Event.InvoiceDeleted
import org.amitayh.invoices.common.domain._
import org.amitayh.invoices.common.serde.AvroSerdeSpec._
import org.apache.kafka.common.serialization.Serde
import org.scalatest.{FunSpec, Matchers, OptionValues}

class AvroSerdeSpec extends FunSpec  with Matchers with OptionValues {

  describe("the CommandResult Avro Serde") {

    val serde: Serde[CommandResult] = AvroSerde.CommandResultSerde

    describe("given a successful CommandResult") {

      val succCommandResult: CommandResult = aSuccessfulCommandResult
      var serialized: Array[Byte] = Array()

      it("should be able to serialize and deserialize back to the same content") {
        assertItCanSerialiseAndDeserializeBack(serde)(succCommandResult)
      }

    }


    describe("given a failed CommandResult containing a VersionMismatch error") {
      val error = VersionMismatch(42, Option(13))
      val failCommandResult: CommandResult = aFailedCommandResult(error)
      it("should be able to serialize and deserialize back to the same content") {
        assertItCanSerialiseAndDeserializeBack(serde)(failCommandResult)
      }
    }

    describe("given a failed CommandResult containing a LineItemDoesNotExist error") {
      val error = LineItemDoesNotExist(42)
      val failCommandResult: CommandResult = aFailedCommandResult(error)
      it("should be able to serialize and deserialize back to the same content") {
        assertItCanSerialiseAndDeserializeBack(serde)(failCommandResult)
      }
    }

    describe("given a failed CommandResult, containing a InvoiceAlreadyExists error") {
      val error = InvoiceAlreadyExists()
      val failCommandResult: CommandResult = aFailedCommandResult(error)
      it("should be able to serialize and deserialize back to the same content") {
        assertItCanSerialiseAndDeserializeBack(serde)(failCommandResult)
      }
    }

    describe("given a failed CommandResult, containing a InvoiceDoesNotExist error") {
      val error = InvoiceDoesNotExist()
      val failCommandResult: CommandResult = aFailedCommandResult(error)
      it("should be able to serialize and deserialize back to the same content") {
        assertItCanSerialiseAndDeserializeBack(serde)(failCommandResult)
      }
    }

    // TODO Test Event Serde
    // TODO Test Command Serde
    // TODO Test InvoiceSnapshot Serde
  }




  def assertItCanSerialiseAndDeserializeBack[T](serde: Serde[T])(t: T) = {
    val ser: Array[Byte] = serde.serializer.serialize("a-topic", t)
    assert(ser.size > 0, "Serialized content should not be empty")
    val des: T = serde.deserializer.deserialize("another-topic", ser)

    des should be(t)
  }
}

object AvroSerdeSpec {

  def anInvoiceDeletedEvent(commandId: UUID): Event = Event(1, Instant.now, commandId, InvoiceDeleted())

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

}

