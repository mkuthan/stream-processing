package org.mkuthan.streamprocessing.toll.application

import com.spotify.scio.io.CustomIO
import com.spotify.scio.testing._

import com.google.api.services.bigquery.model.TableRow
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import org.mkuthan.streamprocessing.shared.scio._
import org.mkuthan.streamprocessing.shared.test.scio._

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
      .input(CustomIO[TableRow]("toll.vehicle_registration"), Seq(anyVehicleRegistrationRawTableRow))
      .output(CustomIO[String](VehicleRegistrationDlqBucketIoId.id)) { results =>
        results should beEmpty
      }
      .output(CustomIO[TableRow]("toll.entry_stats")) { results =>
        results should containSingleValue(anyTollBoothEntryStatsRawTableRow)
      }
      .output(CustomIO[TableRow]("toll.car_total_time")) { results =>
        results should containSingleValue(anyTotalCarTimeRawTableRow)
      }
      .output(CustomIO[String](VehiclesWithExpiredRegistrationTopicIoId.id)) { results =>
        results should beEmpty
      }
      .output(CustomIO[TableRow]("toll.diagnostic")) { results =>
        results should beEmpty
      }
      .run()
  }

  // TODO: e2e scenario for diagnostics, e.g toll entry without toll exit
  // TODO: how to reuse setup between test scenarios and modify only relevant inputs/outputs
}
