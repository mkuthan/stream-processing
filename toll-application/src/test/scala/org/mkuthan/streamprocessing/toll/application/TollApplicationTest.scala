package org.mkuthan.streamprocessing.toll.application

import com.spotify.scio.io.CustomIO
import com.spotify.scio.testing._

import com.google.api.services.bigquery.model.TableRow
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import org.mkuthan.streamprocessing.shared.scio._
import org.mkuthan.streamprocessing.shared.scio.bigquery.BigQueryDeadLetter
import org.mkuthan.streamprocessing.test.scio._
import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothStats
import org.mkuthan.streamprocessing.toll.domain.diagnostic.Diagnostic
import org.mkuthan.streamprocessing.toll.domain.vehicle.TotalVehicleTime

class TollApplicationTest extends AnyFlatSpec with Matchers
    with JobTestScioContext
    with TollApplicationIo
    with TollApplicationMetrics
    with TollApplicationFixtures {

  "Toll application" should "run" in {
    JobTest[TollApplication.type]
      .args(
        "--entrySubscription=projects/any-id/subscriptions/entry-subscription",
        "--entryDlq=gs://entry_dlq",
        "--exitSubscription=projects/any-id/subscriptions/exit-subscription",
        "--exitDlq=gs://exit_dlq",
        "--vehicleRegistrationSubscription=projects/any-id/subscriptions/vehicle-registration-subscription",
        "--vehicleRegistrationTable=toll.vehicle_registration",
        "--vehicleRegistrationDlq=gs://vehicle_registration_dlq",
        "--entryStatsTable=toll.entry_stats",
        "--carTotalTimeTable=toll.car_total_time",
        "--vehiclesWithExpiredRegistrationTopic=vehicles-with-expired-registration",
        "--diagnosticTable=toll.diagnostic"
      )
      // receive toll booth entries and toll booth exists
      .inputStream[PubsubMessage](
        CustomIO[PubsubMessage](EntrySubscriptionIoId.id),
        testStreamOf[PubsubMessage]
          .addElementsAtTime(
            tollBoothEntryTime,
            tollBoothEntryPubsubMessage,
            corruptedJsonPubsubMessage,
            invalidTollBoothEntryPubsubMessage
          )
          .advanceWatermarkToInfinity()
      )
      .counter(TollBoothEntryRawInvalidRows.counter) { value =>
        value should be(1)
      }
      .output(CustomIO[String](EntryDlqBucketIoId.id)) { results =>
        results should containSingleValue(tollBoothEntryDecodingErrorString)
      }
      .inputStream(
        CustomIO[PubsubMessage](ExitSubscriptionIoId.id),
        testStreamOf[PubsubMessage]
          .addElementsAtTime(
            tollBoothExitTime,
            tollBoothExitPubsubMessage,
            corruptedJsonPubsubMessage,
            invalidTollBoothExitPubsubMessage
          ).advanceWatermarkToInfinity()
      )
      .counter(TollBoothExitRawInvalidRows.counter) { value =>
        value should be(1)
      }
      .output(CustomIO[String](ExitDlqBucketIoId.id)) { results =>
        results should containSingleValue(tollBoothExitDecodingErrorString)
      }
      // receive vehicle registrations
      .inputStream(
        CustomIO[PubsubMessage](VehicleRegistrationSubscriptionIoId.id),
        testStreamOf[PubsubMessage]
          // TODO: add event time to vehicle registration messages
          .addElements(anyVehicleRegistrationRawPubsubMessage)
          // TODO: add corrupted json message and check counter
          // TODO: add invalid message and check dead letter
          .advanceWatermarkToInfinity()
      )
      .input(
        CustomIO[TableRow](VehicleRegistrationTableIoId.id),
        Seq(
          // TODO: define another vehicle registration(s) for reading historical data
          anyVehicleRegistrationRawTableRow
        )
      )
      .output(CustomIO[String](VehicleRegistrationDlqBucketIoId.id)) { results =>
        // TODO: add invalid vehicle registration and check dead letter
        results should beEmpty
      }
      // calculate tool booth stats
      .transformOverride(TransformOverride.ofIter[TollBoothStats.Raw, BigQueryDeadLetter[TollBoothStats.Raw]](
        EntryStatsTableIoId.id,
        (r: TollBoothStats.Raw) =>
          // TODO: assert that diagnostic table contains expected rows
          // r should be(anyTollBoothEntryRawTableRow)
          Option.empty[BigQueryDeadLetter[TollBoothStats.Raw]].toList
      ))
      // calculate total vehicle times
      .transformOverride(TransformOverride.ofIter[TotalVehicleTime.Raw, BigQueryDeadLetter[TotalVehicleTime.Raw]](
        TotalVehicleTimeTableIoId.id,
        (r: TotalVehicleTime.Raw) =>
          // TODO: assert that diagnostic table contains expected rows
          // r should be(anyTotalVehicleTimeRawTableRow)
          Option.empty[BigQueryDeadLetter[TotalVehicleTime.Raw]].toList
      ))
      // calculate vehicles with expired registrations
      .output(CustomIO[String](VehiclesWithExpiredRegistrationTopicIoId.id)) { results =>
        // TODO: https://github.com/mkuthan/stream-processing/issues/82
        results should beEmpty
      }
      // pipeline diagnostic
      .transformOverride(TransformOverride.ofIter[Diagnostic.Raw, BigQueryDeadLetter[Diagnostic.Raw]](
        DiagnosticTableIoId.id,
        (r: Diagnostic.Raw) =>
          // TODO: assert that diagnostic table contains expected rows
          Option.empty[BigQueryDeadLetter[Diagnostic.Raw]].toList
      ))
      .run()
  }

  // TODO: how to reuse setup between test scenarios and modify only relevant inputs/outputs
}
