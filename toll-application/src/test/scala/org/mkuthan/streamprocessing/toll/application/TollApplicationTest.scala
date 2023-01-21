package org.mkuthan.streamprocessing.toll.application

import com.spotify.scio.bigquery.BigQueryType
import com.spotify.scio.bigquery.BigQueryTyped
import com.spotify.scio.bigquery.Table
import com.spotify.scio.io.CustomIO
import com.spotify.scio.testing.testStreamOf
import com.spotify.scio.testing.PipelineSpec

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
import org.mkuthan.streamprocessing.toll.infrastructure.json.JsonSerde.writeJson

class TollApplicationTest extends PipelineSpec
    with TollBoothEntryFixture
    with TollBoothExitFixture
    with TollBoothEntryStatsFixture
    with VehicleRegistrationFixture
    with TotalCarTimeFixture {

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
      .inputStream[String](
        CustomIO[String]("projects/any-id/subscriptions/entry-subscription"),
        testStreamOf[String]
          .addElementsAtTime(
            anyTollBoothEntryRaw.entry_time,
            writeJson(anyTollBoothEntryRaw),
            writeJson(tollBoothEntryRawInvalid)
          )
          .advanceWatermarkToInfinity()
      )
      .output(CustomIO[String]("gs://entry_dlq")) { results =>
        results should containSingleValue(writeJson(tollBoothEntryRawInvalid))
      }
      .inputStream(
        CustomIO[String]("projects/any-id/subscriptions/exit-subscription"),
        testStreamOf[String]
          .addElementsAtTime(
            anyTollBoothExitRaw.exit_time,
            writeJson(anyTollBoothExitRaw),
            writeJson(tollBoothExitRawInvalid)
          ).advanceWatermarkToInfinity()
      )
      .output(CustomIO[String]("gs://exit_dlq")) { results =>
        results should containSingleValue(writeJson(tollBoothExitRawInvalid))
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
