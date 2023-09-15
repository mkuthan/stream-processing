package org.mkuthan.streamprocessing.toll.application.streaming

import com.spotify.scio.io.CustomIO
import com.spotify.scio.testing.JobTest

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import org.mkuthan.streamprocessing.infrastructure.common.IoDiagnostic
import org.mkuthan.streamprocessing.infrastructure.pubsub.PubsubDeadLetter
import org.mkuthan.streamprocessing.shared.common.DeadLetter
import org.mkuthan.streamprocessing.shared.common.Message
import org.mkuthan.streamprocessing.test.scio._
import org.mkuthan.streamprocessing.toll.application.io._
import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothEntry
import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothExit
import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothStats
import org.mkuthan.streamprocessing.toll.domain.registration.VehicleRegistration
import org.mkuthan.streamprocessing.toll.domain.vehicle.TotalVehicleTime
import org.mkuthan.streamprocessing.toll.domain.vehicle.TotalVehicleTimeDiagnostic
import org.mkuthan.streamprocessing.toll.domain.vehicle.VehiclesWithExpiredRegistration
import org.mkuthan.streamprocessing.toll.domain.vehicle.VehiclesWithExpiredRegistrationDiagnostic

class TollApplicationTest extends AnyFlatSpec with Matchers
    with JobTestScioContext
    with TollApplicationFixtures {

  // TODO: move to the infra module, but where?
  type PubsubResult[T] = Either[PubsubDeadLetter[T], Message[T]]

  "Toll application" should "run" in {
    JobTest[TollApplication.type]
      .args(
        "--entrySubscription=projects/any-id/subscriptions/entry-subscription",
        "--entryDlq=entry_dlq",
        "--exitSubscription=projects/any-id/subscriptions/exit-subscription",
        "--exitDlq=exit_dlq",
        "--vehicleRegistrationSubscription=projects/any-id/subscriptions/vehicle-registration-subscription",
        "--vehicleRegistrationTable=toll.vehicle_registration",
        "--vehicleRegistrationDlq=vehicle_registration_dlq",
        "--entryStatsTable=toll.entry_stats",
        "--totalVehicleTimeTable=toll.total_vehicle_time",
        "--totalVehicleTimeDiagnosticTable=toll.total_vehicle_time_diagnostic",
        "--vehiclesWithExpiredRegistrationTopic=vehicles-with-expired-registration",
        "--vehiclesWithExpiredRegistrationDiagnosticTable=toll.vehicles_with_expired_registration_diagnostic",
        "--diagnosticTable=toll.io_diagnostic"
      )
      // receive toll booth entries and toll booth exists
      .inputStream[PubsubResult[TollBoothEntry.Raw]](
        CustomIO[PubsubResult[TollBoothEntry.Raw]](EntrySubscriptionIoId.id),
        unboundedTestCollectionOf[PubsubResult[TollBoothEntry.Raw]]
          .addElementsAtTime(
            anyTollBoothEntryRaw.entry_time,
            Right(Message(anyTollBoothEntryRaw)),
            Right(Message(tollBoothEntryRawInvalid)),
            Left(PubsubDeadLetter(
              "corrupted".getBytes,
              Map(),
              "some error"
            ))
          )
          .advanceWatermarkToInfinity().testStream
      )
      .output(CustomIO[DeadLetter[TollBoothEntry.Raw]](EntryDlqBucketIoId.id)) { results =>
        results should containSingleValue(tollBoothEntryDecodingError)
      }
      .inputStream(
        CustomIO[PubsubResult[TollBoothExit.Raw]](ExitSubscriptionIoId.id),
        unboundedTestCollectionOf[PubsubResult[TollBoothExit.Raw]]
          .addElementsAtTime(
            anyTollBoothExitRaw.exit_time,
            Right(Message(anyTollBoothExitRaw)),
            Right(Message(tollBoothExitRawInvalid)),
            Left(PubsubDeadLetter(
              "corrupted".getBytes,
              Map(),
              "some error"
            ))
          ).advanceWatermarkToInfinity().testStream
      )
      .output(CustomIO[DeadLetter[TollBoothExit.Raw]](ExitDlqBucketIoId.id)) { results =>
        results should containSingleValue(tollBoothExitDecodingError)
      }
      // receive vehicle registrations
      .inputStream(
        CustomIO[PubsubResult[VehicleRegistration.Raw]](VehicleRegistrationSubscriptionIoId.id),
        unboundedTestCollectionOf[PubsubResult[VehicleRegistration.Raw]]
          // TODO: add event time to vehicle registration messages
          .addElementsAtWatermarkTime(Right(Message(anyVehicleRegistrationRaw)))
          // TODO: add invalid message and check dead letter
          .advanceWatermarkToInfinity().testStream
      )
      .input(
        CustomIO[VehicleRegistration.Raw](VehicleRegistrationTableIoId.id),
        Seq(
          // TODO: define another vehicle registration(s) for reading historical data
          anyVehicleRegistrationRaw
        )
      )
      .output(CustomIO[String](VehicleRegistrationDlqBucketIoId.id)) { results =>
        // TODO: add invalid vehicle registration and check dead letter
        results should beEmpty
      }
      // calculate tool booth stats
      .output(CustomIO[TollBoothStats.Raw](EntryStatsTableIoId.id)) { results =>
        results should containSingleValue(anyTollBoothStatsRaw)
      }
      // calculate total vehicle times
      .output(CustomIO[TotalVehicleTime.Raw](TotalVehicleTimeTableIoId.id)) { results =>
        results should containSingleValue(anyTotalVehicleTimeRaw)
      }
      .output(CustomIO[TotalVehicleTimeDiagnostic.Raw](TotalVehicleTimeDiagnosticTableIoId.id)) { results =>
        // TODO
        results should beEmpty
      }
      // calculate vehicles with expired registrations
      .output(CustomIO[Message[VehiclesWithExpiredRegistration.Raw]](VehiclesWithExpiredRegistrationTopicIoId.id)) {
        results =>
          results should containSingleValue(Message(anyVehicleWithExpiredRegistrationRaw))
      }
      .output(CustomIO[VehiclesWithExpiredRegistrationDiagnostic.Raw](
        VehiclesWithExpiredRegistrationDiagnosticTableIoId.id
      )) {
        results =>
          // TODO
          results should beEmpty
      }
      .output(CustomIO[IoDiagnostic.Raw](DiagnosticTableIoId.id)) { results =>
        // toll booth entry and toll booth exit
        results should haveSize(2)
      }
      .run()
  }

  // TODO: how to reuse setup between test scenarios and modify only relevant inputs/outputs
}
