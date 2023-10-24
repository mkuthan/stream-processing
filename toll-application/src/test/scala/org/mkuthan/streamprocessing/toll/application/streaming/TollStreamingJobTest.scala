package org.mkuthan.streamprocessing.toll.application.streaming

import com.spotify.scio.io.CustomIO
import com.spotify.scio.testing.JobTest

import org.joda.time.Instant
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import org.mkuthan.streamprocessing.infrastructure.pubsub.syntax._
import org.mkuthan.streamprocessing.shared.common.DeadLetter
import org.mkuthan.streamprocessing.shared.common.Diagnostic
import org.mkuthan.streamprocessing.shared.common.Message
import org.mkuthan.streamprocessing.test.scio.syntax._
import org.mkuthan.streamprocessing.test.scio.JobTestScioContext
import org.mkuthan.streamprocessing.toll.application.TollJobFixtures
import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothDiagnostic
import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothEntry
import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothExit
import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothStats
import org.mkuthan.streamprocessing.toll.domain.registration.VehicleRegistration
import org.mkuthan.streamprocessing.toll.domain.vehicle.TotalVehicleTimes
import org.mkuthan.streamprocessing.toll.domain.vehicle.VehiclesWithExpiredRegistration

class TollStreamingJobTest extends AnyFlatSpec with Matchers
    with JobTestScioContext
    with TollJobFixtures
    with TollStreamingJobIo {

  "Toll job" should "run in the streaming mode" in {
    JobTest[TollStreamingJob.type]
      .args(
        "--effectiveDate=2014-09-10",
        "--entrySubscription=projects/any-id/subscriptions/entry-subscription",
        "--entryDlq=entry_dlq",
        "--exitSubscription=projects/any-id/subscriptions/exit-subscription",
        "--exitDlq=exit_dlq",
        "--vehicleRegistrationSubscription=projects/any-id/subscriptions/vehicle-registration-subscription",
        "--vehicleRegistrationDlq=vehicle_registration_dlq",
        "--vehicleRegistrationTable=toll.vehicle_registration",
        "--entryStatsTable=toll.entry_stats",
        "--totalVehicleTimesTable=toll.total_vehicle_time",
        "--totalVehicleTimesDiagnosticTable=toll.total_vehicle_time_diagnostic",
        "--vehiclesWithExpiredRegistrationTopic=projects/any-id/topics/vehicles-with-expired-registration",
        "--vehiclesWithExpiredRegistrationDiagnosticTable=toll.vehicles_with_expired_registration_diagnostic",
        "--ioDiagnosticTable=toll.io_diagnostic"
      )
      // receive toll booth entries and toll booth exists
      .inputStream[PubsubResult[TollBoothEntry.Payload]](
        CustomIO[PubsubResult[TollBoothEntry.Payload]](EntrySubscriptionIoId.id),
        unboundedTestCollectionOf[PubsubResult[TollBoothEntry.Payload]]
          .addElementsAtTime(
            anyTollBoothEntryMessage.attributes(TollBoothEntry.TimestampAttribute),
            Right(anyTollBoothEntryMessage),
            Right(invalidTollBoothEntryMessage),
            Left(tollBoothEntryPubsubDeadLetter)
          )
          .advanceWatermarkToInfinity().testStream
      )
      .output(CustomIO[DeadLetter[TollBoothEntry.Payload]](EntryDlqBucketIoId.id)) { results =>
        results should containElements(tollBoothEntryDecodingError)
      }
      .inputStream(
        CustomIO[PubsubResult[TollBoothExit.Payload]](ExitSubscriptionIoId.id),
        unboundedTestCollectionOf[PubsubResult[TollBoothExit.Payload]]
          .addElementsAtTime(
            anyTollBoothExitMessage.attributes(TollBoothExit.TimestampAttribute),
            Right(anyTollBoothExitMessage),
            Right(invalidTollBoothExitMessage),
            Left(tollBoothExitPubsubDeadLetter)
          ).advanceWatermarkToInfinity().testStream
      )
      .output(CustomIO[DeadLetter[TollBoothExit.Payload]](ExitDlqBucketIoId.id)) { results =>
        results should containElements(tollBoothExitDecodingError)
      }
      // receive vehicle registrations
      .inputStream(
        CustomIO[PubsubResult[VehicleRegistration.Payload]](VehicleRegistrationSubscriptionIoId.id),
        unboundedTestCollectionOf[PubsubResult[VehicleRegistration.Payload]]
          .addElementsAtTime(
            anyVehicleRegistrationMessage.attributes(VehicleRegistration.TimestampAttribute),
            Right(anyVehicleRegistrationMessage),
            Right(invalidVehicleRegistrationMessage),
            Left(vehicleRegistrationPubsubDeadLetter)
          )
          .advanceWatermarkToInfinity().testStream
      )
      .input(
        CustomIO[VehicleRegistration.Record](VehicleRegistrationTableIoId.id),
        Seq(anyVehicleRegistrationRecord)
      )
      .output(CustomIO[DeadLetter[VehicleRegistration.Payload]](VehicleRegistrationDlqBucketIoId.id)) { results =>
        results should containElements(vehicleRegistrationDecodingError)
      }
      // calculate tool booth stats
      .output(CustomIO[TollBoothStats.Record](EntryStatsTableIoId.id)) { results =>
        results should containElements(
          anyTollBoothStatsRecord.copy(created_at = Instant.parse("2014-09-10T12:09:59.999Z"))
        )
      }
      // calculate total vehicle times
      .output(CustomIO[TotalVehicleTimes.Record](TotalVehicleTimesTableIoId.id)) { results =>
        results should containElements(
          anyTotalVehicleTimesRecord.copy(created_at = Instant.parse("2014-09-10T12:12:59.999Z"))
        )
      }
      .output(CustomIO[TollBoothDiagnostic.Record](TotalVehicleTimesDiagnosticTableIoId.id)) { results =>
        results should beEmpty
      }
      // calculate vehicles with expired registrations
      .output(CustomIO[Message[VehiclesWithExpiredRegistration.Payload]](VehiclesWithExpiredRegistrationTopicIoId.id)) {
        results =>
          val createdAt = Instant.parse("2014-09-10T12:01:00Z") // entry time
          results should containElements(
            anyVehicleWithExpiredRegistrationMessage(createdAt, anyVehicleRegistrationMessage.payload.id),
            anyVehicleWithExpiredRegistrationMessage(createdAt, anyVehicleRegistrationRecord.id)
          )
      }
      .output(CustomIO[TollBoothDiagnostic.Record](
        VehiclesWithExpiredRegistrationDiagnosticTableIoId.id
      )) {
        results =>
          results should beEmpty
      }
      .output(CustomIO[Diagnostic.Record](IoDiagnosticTableIoId.id)) { results =>
        results should containElements(
          anyIoDiagnosticRecord.copy(
            created_at = Instant.parse("2014-09-10T12:09:59.999Z"),
            id = EntrySubscriptionIoId.id,
            reason = tollBoothEntryPubsubDeadLetter.error
          ),
          anyIoDiagnosticRecord.copy(
            created_at = Instant.parse("2014-09-10T12:09:59.999Z"),
            id = ExitSubscriptionIoId.id,
            reason = tollBoothExitPubsubDeadLetter.error
          ),
          anyIoDiagnosticRecord.copy(
            created_at = Instant.parse("2014-09-10T11:59:59.999Z"),
            id = VehicleRegistrationSubscriptionIoId.id,
            reason = vehicleRegistrationPubsubDeadLetter.error
          )
        )
      }
      .run()
  }
}
