package org.mkuthan.streamprocessing.toll.application

import com.spotify.scio.bigquery.BigQueryType
import com.spotify.scio.bigquery.BigQueryTyped
import com.spotify.scio.bigquery.Table
import com.spotify.scio.io.CustomIO
import com.spotify.scio.testing.PipelineSpec

import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothEntry
import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothEntryStats
import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothExit
import org.mkuthan.streamprocessing.toll.domain.diagnostic.Diagnostic
import org.mkuthan.streamprocessing.toll.domain.registration.VehicleRegistration
import org.mkuthan.streamprocessing.toll.domain.toll.TotalCarTime
import org.mkuthan.streamprocessing.toll.infrastructure.json.JsonSerde

class TollApplicationTest extends PipelineSpec {
  val tollBoothEntry = TollBoothEntry.Raw(
    id = "1",
    entry_time = "2014-09-10T12:01:00.000Z",
    license_plate = "JNB 7001",
    state = "NY",
    make = "Honda",
    model = "CRV",
    vehicle_type = "1",
    weight_type = "0",
    toll = "7",
    tag = "String"
  )
  val tollBoothExit = TollBoothExit.Raw(
    id = "1",
    exit_time = "2014-09-10T12:03:00.0000000Z",
    license_plate = "JNB 7001"
  )
  val vehicleRegistration = VehicleRegistration.Raw(
    id = "1",
    licence_plate = "JNB 7001",
    expired = 0
  )

  "Toll application" should "run" in {
    JobTest[TollApplication.type]
      .args(
        "--entrySubscription=projects/any-id/subscriptions/entry-subscription",
        "--entryDlq=gs://entry_dlq",
        "--exitSubscription=projects/any-id/subscriptions/exit-subscription",
        "--exitDlq=gs://exit_dlq",
        "--vehicleRegistrationTable=toll.vehicle_registration",
        "--vehicleRegistrationDlq=gs://vehicle_registration_dlq",
        "--entryCountTable=toll.entry_count",
        "--carTotalTimeTable=toll.car_total_time",
        "--vehiclesWithExpiredRegistrationTopic=vehicles-with-expired-registration",
        "--diagnosticTable=toll.diagnostic"
      )
      .input(
        CustomIO[String]("projects/any-id/subscriptions/entry-subscription"),
        Seq(JsonSerde.write(tollBoothEntry))
      )
      .output(CustomIO[TollBoothEntry.Raw]("gs://entry_dlq")) { results =>
        results should beEmpty
      }
      .input(
        CustomIO[String]("projects/any-id/subscriptions/exit-subscription"),
        Seq(JsonSerde.write(tollBoothExit))
      )
      .output(CustomIO[String]("gs://exit_dlq")) { results =>
        results should beEmpty
      }
      .input(
        BigQueryTyped.Storage[VehicleRegistration.Raw](
          Table.Spec("toll.vehicle_registration"),
          BigQueryType[VehicleRegistration.Raw].selectedFields.get, // TODO
          BigQueryType[VehicleRegistration.Raw].rowRestriction
        ),
        Seq(vehicleRegistration)
      )
      .output(CustomIO[String]("gs://vehicle_registration_dlq")) { results =>
        results should beEmpty
      }
      .output(BigQueryTyped.Table[TollBoothEntryStats.Raw](Table.Spec("toll.entry_count"))) { results =>
        results should beEmpty
      }
      .output(BigQueryTyped.Table[TotalCarTime.Raw](Table.Spec("toll.car_total_time"))) { results =>
        results should beEmpty
      }
      .output(CustomIO[String]("vehicles-with-expired-registration")) { results =>
        results should beEmpty
      }
      .output(BigQueryTyped.Table[Diagnostic.Raw](Table.Spec("toll.diagnostic"))) { results =>
        results should beEmpty
      }
      .run()
  }
}
