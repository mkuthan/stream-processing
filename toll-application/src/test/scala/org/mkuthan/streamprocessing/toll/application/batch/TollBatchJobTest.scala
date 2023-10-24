package org.mkuthan.streamprocessing.toll.application.batch

import com.spotify.scio.io.CustomIO
import com.spotify.scio.testing.JobTest

import org.joda.time.Instant
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import org.mkuthan.streamprocessing.test.scio.JobTestScioContext
import org.mkuthan.streamprocessing.toll.application.TollJobFixtures
import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothDiagnostic
import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothEntry
import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothExit
import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothStats
import org.mkuthan.streamprocessing.toll.domain.registration.VehicleRegistration
import org.mkuthan.streamprocessing.toll.domain.vehicle.TotalVehicleTimes
import org.mkuthan.streamprocessing.toll.domain.vehicle.VehiclesWithExpiredRegistration

class TollBatchJobTest extends AnyFlatSpec with Matchers
    with JobTestScioContext
    with TollJobFixtures
    with TollBatchJobIo {

  "Toll job" should "run in the batch mode" in {
    JobTest[TollBatchJob.type]
      .args(
        "--effectiveDate=2014-09-10",
        "--entryTable=toll.entry",
        "--exitTable=toll.exit",
        "--vehicleRegistrationTable=toll.vehicle_registration",
        "--entryStatsHourlyTable=toll.entry_stats_hourly",
        "--entryStatsDailyTable=toll.entry_stats_daily",
        "--totalVehicleTimesOneHourGapTable=toll.total_vehicle_time_one_hour_gap",
        "--totalVehicleTimesDiagnosticOneHourGapTable=toll.total_vehicle_time_diagnostic_one_hour_gap",
        "--vehiclesWithExpiredRegistrationDailyTable=toll.vehicles_with_expired_registration_daily",
        "--vehiclesWithExpiredRegistrationDiagnosticDailyTable=toll.vehicles_with_expired_registration_diagnostic_daily"
      )
      // read toll booth entries and toll booth exists
      .input(CustomIO[TollBoothEntry.Record](EntryTableIoId.id), Seq(anyTollBoothEntryRecord))
      .input(CustomIO[TollBoothExit.Record](ExitTableIoId.id), Seq(anyTollBoothExitRecord))
      // read vehicle registrations
      .input(CustomIO[VehicleRegistration.Record](VehicleRegistrationTableIoId.id), Seq(anyVehicleRegistrationRecord))
      // calculate tool booth stats
      .output(CustomIO[TollBoothStats.Record](EntryStatsHourlyTableIoId.id)) { results =>
        results should containElements(
          anyTollBoothStatsRecord.copy(created_at = Instant.parse("2014-09-10T12:59:59.999Z"))
        )
      }
      .output(CustomIO[TollBoothStats.Record](EntryStatsDailyTableIoId.id)) { results =>
        results should containElements(
          anyTollBoothStatsRecord.copy(created_at = Instant.parse("2014-09-10T23:59:59.999Z"))
        )
      }
      // calculate total vehicle times
      .output(CustomIO[TotalVehicleTimes.Record](TotalVehicleTimesOneHourGapTableIoId.id)) { results =>
        results should containElements(
          anyTotalVehicleTimesRecord.copy(created_at = Instant.parse("2014-09-10T13:02:59.999Z"))
        )
      }
      .output(CustomIO[TollBoothDiagnostic.Record](TotalVehicleTimesDiagnosticOneHourGapTableIoId.id)) {
        results =>
          results should beEmpty
      }
      // calculate vehicles with expired registrations
      .output(CustomIO[VehiclesWithExpiredRegistration.Record](VehiclesWithExpiredRegistrationDailyTableIoId.id)) {
        results =>
          val createdAt = Instant.parse("2014-09-10T12:01:00Z")
          results should containElements(
            anyVehicleWithExpiredRegistrationRecord(createdAt, anyVehicleRegistrationRecord.id)
          )
      }
      .output(CustomIO[TollBoothDiagnostic.Record](
        VehiclesWithExpiredRegistrationDiagnosticDailyTableIoId.id
      )) {
        results =>
          results should beEmpty
      }
      .run()
  }
}
