package org.mkuthan.streamprocessing.toll.application.streaming

import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage

import com.spotify.scio.io.CustomIO
import com.spotify.scio.testing.JobTest

import org.scalactic.Equality
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import org.mkuthan.streamprocessing.infrastructure._
import org.mkuthan.streamprocessing.infrastructure.diagnostic.IoDiagnostic
import org.mkuthan.streamprocessing.test.scio._
import org.mkuthan.streamprocessing.toll.application.io._
import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothStats
import org.mkuthan.streamprocessing.toll.domain.registration.VehicleRegistration
import org.mkuthan.streamprocessing.toll.domain.vehicle.TotalVehicleTime
import org.mkuthan.streamprocessing.toll.domain.vehicle.TotalVehicleTimeDiagnostic
import org.mkuthan.streamprocessing.toll.domain.vehicle.VehiclesWithExpiredRegistrationDiagnostic

class TollApplicationTest extends AnyFlatSpec with Matchers
    with JobTestScioContext
    with TollApplicationFixtures {

  implicit val pubsubMessageEquality: Equality[PubsubMessage] =
    (a: PubsubMessage, b: Any) =>
      (a, b) match {
        case (a: PubsubMessage, b: PubsubMessage) =>
          a.getPayload.sameElements(b.getPayload) && a.getAttributeMap == b.getAttributeMap
        case _ => false
      }

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
      .inputStream[PubsubMessage](
        CustomIO[PubsubMessage](EntrySubscriptionIoId.id),
        unboundedTestCollectionOf[PubsubMessage]
          .addElementsAtTime(
            tollBoothEntryTime,
            tollBoothEntryPubsubMessage,
            corruptedJsonPubsubMessage,
            invalidTollBoothEntryPubsubMessage
          )
          .advanceWatermarkToInfinity().testStream
      )
      .output(CustomIO[String](EntryDlqBucketIoId.id)) { results =>
        results should containSingleValue(tollBoothEntryDecodingErrorString)
      }
      .inputStream(
        CustomIO[PubsubMessage](ExitSubscriptionIoId.id),
        unboundedTestCollectionOf[PubsubMessage]
          .addElementsAtTime(
            tollBoothExitTime,
            tollBoothExitPubsubMessage,
            corruptedJsonPubsubMessage,
            invalidTollBoothExitPubsubMessage
          ).advanceWatermarkToInfinity().testStream
      )
      .output(CustomIO[String](ExitDlqBucketIoId.id)) { results =>
        results should containSingleValue(tollBoothExitDecodingErrorString)
      }
      // receive vehicle registrations
      .inputStream(
        CustomIO[PubsubMessage](VehicleRegistrationSubscriptionIoId.id),
        unboundedTestCollectionOf[PubsubMessage]
          // TODO: add event time to vehicle registration messages
          .addElementsAtWatermarkTime(anyVehicleRegistrationRawPubsubMessage)
          // TODO: add corrupted json message and check counter
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
      .output(CustomIO[PubsubMessage](VehiclesWithExpiredRegistrationTopicIoId.id)) { results =>
        results should containSingleValue(anyVehicleWithExpiredRegistrationRawPubsubMessage)
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
