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
      .inputStream[PubsubMessage](
        CustomIO[PubsubMessage](EntrySubscriptionIoId.id),
        testStreamOf[PubsubMessage]
          .addElementsAtTime(
            tollBoothEntryTime,
            tollBoothEntryPubsubMessage,
            invalidTollBoothEntryPubsubMessage,
            corruptedJsonPubsubMessage
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
            invalidTollBoothExitPubsubMessage,
            corruptedJsonPubsubMessage
          ).advanceWatermarkToInfinity()
      )
      .counter(TollBoothExitRawInvalidRows.counter) { value =>
        value should be(1)
      }
      .output(CustomIO[String](ExitDlqBucketIoId.id)) { results =>
        results should containSingleValue(tollBoothExitDecodingErrorString)
      }
      .inputStream(
        CustomIO[PubsubMessage](VehicleRegistrationSubscriptionIoId.id),
        testStreamOf[PubsubMessage]
          .addElements(anyVehicleRegistrationRawPubsubMessage)
          .advanceWatermarkToInfinity()
      )
      .input(CustomIO[TableRow](VehicleRegistrationTableIoId.id), Seq(anyVehicleRegistrationRawTableRow))
      .output(CustomIO[String](VehicleRegistrationDlqBucketIoId.id)) { results =>
        results should beEmpty
      }
      .transformOverride(TransformOverride.ofIter[TollBoothStats.Raw, BigQueryDeadLetter[TollBoothStats.Raw]](
        EntryStatsTableIoId.id,
        (r: TollBoothStats.Raw) =>
          // TODO: assert that diagnostic table contains expected rows
          // r should be(anyTollBoothEntryRawTableRow)
          Option.empty[BigQueryDeadLetter[TollBoothStats.Raw]].toList
      ))
      .transformOverride(TransformOverride.ofIter[TotalVehicleTime.Raw, BigQueryDeadLetter[TotalVehicleTime.Raw]](
        TotalVehicleTimeTableIoId.id,
        (r: TotalVehicleTime.Raw) =>
          // TODO: assert that diagnostic table contains expected rows
          // r should be(anyTotalVehicleTimeRawTableRow)
          Option.empty[BigQueryDeadLetter[TotalVehicleTime.Raw]].toList
      ))
      .output(CustomIO[String](VehiclesWithExpiredRegistrationTopicIoId.id)) { results =>
        results should beEmpty
      }
      .transformOverride(TransformOverride.ofIter[Diagnostic.Raw, BigQueryDeadLetter[Diagnostic.Raw]](
        DiagnosticTableIoId.id,
        (r: Diagnostic.Raw) =>
          // TODO: assert that diagnostic table contains expected rows
          Option.empty[BigQueryDeadLetter[Diagnostic.Raw]].toList
      ))
      .run()
  }

  // TODO: e2e scenario for diagnostics, e.g toll entry without toll exit
  // TODO: how to reuse setup between test scenarios and modify only relevant inputs/outputs
}
