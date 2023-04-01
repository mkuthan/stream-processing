package org.mkuthan.streamprocessing.toll.application

import com.spotify.scio.bigquery.BigQueryType
import com.spotify.scio.bigquery.BigQueryTyped
import com.spotify.scio.bigquery.Table
import com.spotify.scio.coders.Coder
import com.spotify.scio.io.CustomIO
import com.spotify.scio.testing.testStreamOf
import com.spotify.scio.testing.JobTest

import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessageWithAttributesCoder
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import org.mkuthan.streamprocessing.shared.test.scio._
import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothEntryFixture
import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothEntryStats
import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothEntryStatsFixture
import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothExitFixture
import org.mkuthan.streamprocessing.toll.domain.diagnostic.Diagnostic
import org.mkuthan.streamprocessing.toll.domain.registration.VehicleRegistration
import org.mkuthan.streamprocessing.toll.domain.registration.VehicleRegistrationFixture
import org.mkuthan.streamprocessing.toll.domain.toll.TotalCarTime
import org.mkuthan.streamprocessing.toll.domain.toll.TotalCarTimeFixture
import org.mkuthan.streamprocessing.toll.infrastructure.json.JsonSerde.writeJsonAsBytes
import org.mkuthan.streamprocessing.toll.infrastructure.json.JsonSerde.writeJsonAsString

class TollApplicationTest extends AnyFlatSpec with Matchers
    with JobTestScioContext
    with TollBoothEntryFixture
    with TollBoothExitFixture
    with TollBoothEntryStatsFixture
    with VehicleRegistrationFixture
    with TotalCarTimeFixture {

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
            new PubsubMessage(writeJsonAsBytes(anyTollBoothEntryRaw), null),
            new PubsubMessage(writeJsonAsBytes(tollBoothEntryRawInvalid), null)
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
            new PubsubMessage(writeJsonAsBytes(anyTollBoothExitRaw), null),
            new PubsubMessage(writeJsonAsBytes(tollBoothExitRawInvalid), null)
          ).advanceWatermarkToInfinity()
      )
      .output(CustomIO[String]("gs://exit_dlq")) { results =>
        results should containSingleValue(writeJsonAsString(tollBoothExitDecodingError))
      }
      .input(
        BigQueryTyped.Storage[VehicleRegistration.Raw](
          Table.Spec("toll.vehicle_registration"),
          // TODO: hide this complexity
          BigQueryType[VehicleRegistration.Raw].selectedFields.get,
          BigQueryType[VehicleRegistration.Raw].rowRestriction
        ),
        Seq(anyVehicleRegistrationRaw)
      )
      .output(CustomIO[String]("gs://vehicle_registration_dlq")) { results =>
        results should beEmpty
      }
      .output(BigQueryTyped.Table[TollBoothEntryStats.Raw](Table.Spec("toll.entry_stats"))) { results =>
        results should containSingleValue(anyTollBoothEntryStatsRaw)
      }
      .output(BigQueryTyped.Table[TotalCarTime.Raw](Table.Spec("toll.car_total_time"))) { results =>
        results should containSingleValue(anyTotalCarTimeRaw)
      }
      .output(CustomIO[String]("vehicles-with-expired-registration")) { results =>
        results should beEmpty
      }
      .output(BigQueryTyped.Table[Diagnostic.Raw](Table.Spec("toll.diagnostic"))) { results =>
        results should beEmpty
      }
      .run()
  }

  // TODO: e2e scenario for diagnostics, e.g toll entry without toll exit
  // TODO: how to reuse setup between test scenarios and modify only relevant inputs/outputs
}
