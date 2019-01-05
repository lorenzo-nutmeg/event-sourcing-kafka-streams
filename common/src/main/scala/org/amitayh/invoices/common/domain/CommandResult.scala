package org.amitayh.invoices.common.domain

import java.time.Instant
import java.util.UUID

/**
  * Result of a Command.
  * Outcome may be Success, wrapping:
  *   - the original state of the snapshot
  *   - the final state of the snapshot
  *   - the list of events
  * or Failure, wrapping the error
  */
case class CommandResult(originId: UUID,
                         commandId: UUID,
                         outcome: CommandResult.Outcome)

object CommandResult {
  sealed trait Outcome

  case class Success(events: Vector[Event],
                     oldSnapshot: InvoiceSnapshot,
                     newSnapshot: InvoiceSnapshot) extends Outcome {

    def update(timestamp: Instant,
               commandId: UUID,
               payload: Event.Payload): Success = {

      // The Event version is always equal to the original snapshot version + the number of this event (1..n)
      // For example: if a Command generates a single Event, the Event version is the version of the previous snapshot + 1
      val event = Event(nextVersion, timestamp, commandId, payload)

      // The SnapshotReducer applies events to the snapshot (actually using InvoiceReducer to update the state of the Invoice)
      // but only after checking whether the version of the event matches the version of the snapshot (-1)
      // it is going to be applied to.
      // This is not really useful here, as we just generated the Event version accordingly. But I guess the Snapshot reducer
      // is supposed to be more general, to apply any event to any snapshot
      val snapshot = SnapshotReducer.handle(newSnapshot, event)
      copy(events = events :+ event, newSnapshot = snapshot)
    }

    private def nextVersion: Int =
      oldSnapshot.version + events.length + 1

  }

  object Success {
    def apply(snapshot: InvoiceSnapshot): Success =
      Success(Vector.empty, snapshot, snapshot)
  }

  case class Failure(cause: InvoiceError) extends Outcome
}
