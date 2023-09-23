package org.mkuthan.streamprocessing.toll.application.streaming

import com.spotify.scio.io.CustomIO
import com.spotify.scio.testing.JobTest

import org.joda.time.Instant
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import org.mkuthan.streamprocessing.infrastructure.common.IoDiagnostic
import org.mkuthan.streamprocessing.infrastructure.pubsub.syntax._
import org.mkuthan.streamprocessing.infrastructure.pubsub.PubsubDeadLetter
import org.mkuthan.streamprocessing.shared.common.DeadLetter
import org.mkuthan.streamprocessing.shared.common.Message
import org.mkuthan.streamprocessing.test.scio._
import org.mkuthan.streamprocessing.toll.application.TollJobFixtures
import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothEntry
import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothExit
import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothStats
import org.mkuthan.streamprocessing.toll.domain.registration.VehicleRegistration
import org.mkuthan.streamprocessing.toll.domain.vehicle.TotalVehicleTime
import org.mkuthan.streamprocessing.toll.domain.vehicle.TotalVehicleTimeDiagnostic
import org.mkuthan.streamprocessing.toll.domain.vehicle.VehiclesWithExpiredRegistration
import org.mkuthan.streamprocessing.toll.domain.vehicle.VehiclesWithExpiredRegistrationDiagnostic

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
      .inputStream[PubsubResult[TollBoothEntry.Payload]](
        CustomIO[PubsubResult[TollBoothEntry.Payload]](EntrySubscriptionIoId.id),
        unboundedTestCollectionOf[PubsubResult[TollBoothEntry.Payload]]
          .addElementsAtTime(
            anyTollBoothEntryPayload.entry_time,
            Right(Message(anyTollBoothEntryPayload)),
            Right(Message(tollBoothEntryPayloadInvalid)),
            Left(PubsubDeadLetter(
              "corrupted".getBytes,
              Map(),
              "some error"
            ))
          )
          .advanceWatermarkToInfinity().testStream
      )
      .output(CustomIO[DeadLetter[TollBoothEntry.Payload]](EntryDlqBucketIoId.id)) { results =>
        results should containSingleValue(tollBoothEntryDecodingError)
      }
      .inputStream(
        CustomIO[PubsubResult[TollBoothExit.Payload]](ExitSubscriptionIoId.id),
        unboundedTestCollectionOf[PubsubResult[TollBoothExit.Payload]]
          .addElementsAtTime(
            anyTollBoothExitPayload.exit_time,
            Right(Message(anyTollBoothExitPayload)),
            Right(Message(tollBoothExitPayloadInvalid)),
            Left(PubsubDeadLetter(
              "corrupted".getBytes,
              Map(),
              "some error"
            ))
          ).advanceWatermarkToInfinity().testStream
      )
      .output(CustomIO[DeadLetter[TollBoothExit.Payload]](ExitDlqBucketIoId.id)) { results =>
        results should containSingleValue(tollBoothExitDecodingError)
      }
      // receive vehicle registrations
      .inputStream(
        CustomIO[PubsubResult[VehicleRegistration.Payload]](VehicleRegistrationSubscriptionIoId.id),
        unboundedTestCollectionOf[PubsubResult[VehicleRegistration.Payload]]
          .addElementsAtTime(
            anyVehicleRegistrationMessage.attributes(VehicleRegistration.TimestampAttribute),
            Right(anyVehicleRegistrationMessage)
          )
          // TODO: add invalid message and check dead letter
          .advanceWatermarkToInfinity().testStream
      )
      .input(
        CustomIO[VehicleRegistration.Record](VehicleRegistrationTableIoId.id),
        Seq(
          anyVehicleRegistrationRecord
        )
      )
      .output(CustomIO[String](VehicleRegistrationDlqBucketIoId.id)) { results =>
        // TODO: add invalid vehicle registration and check dead letter
        results should beEmpty
      }
      // calculate tool booth stats
      .output(CustomIO[TollBoothStats.Record](EntryStatsTableIoId.id)) { results =>
        results should containSingleValue(
          anyTollBoothStatsRecord.copy(created_at = Instant.parse("2014-09-10T12:09:59.999Z"))
        )
      }
      // calculate total vehicle times
      .output(CustomIO[TotalVehicleTime.Record](TotalVehicleTimeTableIoId.id)) { results =>
        results should containSingleValue(
          anyTotalVehicleTimeRecord.copy(created_at = Instant.parse("2014-09-10T12:12:59.999Z"))
        )
      }
      .output(CustomIO[TotalVehicleTimeDiagnostic.Record](TotalVehicleTimeDiagnosticTableIoId.id)) { results =>
        // TODO
        results should beEmpty
      }
      // calculate vehicles with expired registrations
      .output(CustomIO[Message[VehiclesWithExpiredRegistration.Payload]](VehiclesWithExpiredRegistrationTopicIoId.id)) {
        results =>
          val createdAt = Instant.parse("2014-09-10T12:01:00Z") // entry time
          results should containInAnyOrder(Seq(
            anyVehicleWithExpiredRegistrationMessage(createdAt, anyVehicleRegistrationMessage.payload.id),
            anyVehicleWithExpiredRegistrationMessage(createdAt, anyVehicleRegistrationRecord.id)
          ))
      }
      .output(CustomIO[VehiclesWithExpiredRegistrationDiagnostic.Record](
        VehiclesWithExpiredRegistrationDiagnosticTableIoId.id
      )) {
        results =>
          // TODO
          results should beEmpty
      }
      .output(CustomIO[IoDiagnostic.Record](DiagnosticTableIoId.id)) { results =>
        // toll booth entry and toll booth exit
        results should haveSize(2)
      }
      .run()
  }

  // TODO: how to reuse setup between test scenarios and modify only relevant inputs/outputs
}
