package org.mkuthan.streamprocessing.toll.application

import com.spotify.scio.coders.Coder
import com.spotify.scio.io.CustomIO
import com.spotify.scio.testing._

import com.google.api.services.bigquery.model.TableRow
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessageWithAttributesCoder
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import org.mkuthan.streamprocessing.shared.test.scio._
import org.mkuthan.streamprocessing.toll.infrastructure.json.JsonSerde.writeJsonAsString

class TollApplicationTest extends AnyFlatSpec with Matchers
    with JobTestScioContext
    with TollApplicationFixtures {

  // TODO: move somewhere
  implicit def messageCoder: Coder[PubsubMessage] =
    Coder.beam(PubsubMessageWithAttributesCoder.of())

  "Toll application" should "run" in {
    JobTest[TollApplication.type]
      .args(
        "--entrySubscription=projects/any-id/subscriptions/entry-subscription",
        "--entryDlq=gs://entry_dlq",
        "--exitSubscription=projects/any-id/subscriptions/exit-subscription",
        "--exitDlq=gs://exit_dlq",
        "--vehicleRegistrationTable=toll.vehicle_registration",
        "--vehicleRegistrationDlq=gs://vehicle_registration_dlq",
        "--entryStatsTable=toll.entry_stats",
        "--carTotalTimeTable=toll.car_total_time",
        "--vehiclesWithExpiredRegistrationTopic=vehicles-with-expired-registration",
        "--diagnosticTable=toll.diagnostic"
      )
      .inputStream[PubsubMessage](
        CustomIO[PubsubMessage]("projects/any-id/subscriptions/entry-subscription"),
        testStreamOf[PubsubMessage]
          .addElementsAtTime(
            anyTollBoothEntryRaw.entry_time,
            new PubsubMessage(anyTollBoothEntryRawJson, null),
            new PubsubMessage(tollBoothEntryRawInvalidJson, null)
          )
          .advanceWatermarkToInfinity()
      )
      .output(CustomIO[String]("gs://entry_dlq")) { results =>
        results should containSingleValue(writeJsonAsString(tollBoothEntryDecodingError))
      }
      .inputStream(
        CustomIO[PubsubMessage]("projects/any-id/subscriptions/exit-subscription"),
        testStreamOf[PubsubMessage]
          .addElementsAtTime(
            anyTollBoothExitRaw.exit_time,
            new PubsubMessage(anyTollBoothExitRawJson, null),
            new PubsubMessage(tollBoothExitRawInvalidJson, null)
          ).advanceWatermarkToInfinity()
      )
      .output(CustomIO[String]("gs://exit_dlq")) { results =>
        results should containSingleValue(writeJsonAsString(tollBoothExitDecodingError))
      }
      .input(CustomIO[TableRow]("toll.vehicle_registration"), Seq(anyVehicleRegistrationRawTableRow))
      .output(CustomIO[String]("gs://vehicle_registration_dlq")) { results =>
        results should beEmpty
      }
      .transformOverride(TransformOverride.of[TableRow, Unit](
        "toll.entry_stats",
        (r: TableRow) => {
          r should be(anyTollBoothEntryStatsRawTableRow)
          ()
        }
      ))
      .transformOverride(TransformOverride.of[TableRow, Unit](
        "toll.car_total_time",
        (r: TableRow) => {
          r should be(anyTotalCarTimeRawTableRow)
          ()
        }
      ))
      .output(CustomIO[String]("vehicles-with-expired-registration")) { results =>
        results should beEmpty
      }
      .transformOverride(TransformOverride.of[TableRow, Unit](
        "toll.diagnostic",
        (r: TableRow) => {
          println(r)
          ()
        }
      ))
      .run()
  }

  // TODO: e2e scenario for diagnostics, e.g toll entry without toll exit
  // TODO: how to reuse setup between test scenarios and modify only relevant inputs/outputs
}
