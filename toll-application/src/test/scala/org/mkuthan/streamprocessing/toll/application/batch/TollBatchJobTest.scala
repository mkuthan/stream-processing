package org.mkuthan.streamprocessing.toll.application.batch

import com.spotify.scio.io.CustomIO
import com.spotify.scio.testing.JobTest

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import org.mkuthan.streamprocessing.test.scio.JobTestScioContext
import org.mkuthan.streamprocessing.toll.application.TollJobFixtures
import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothEntry
import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothStats
import org.mkuthan.streamprocessing.toll.domain.vehicle.TotalVehicleTime

class TollBatchJobTest extends AnyFlatSpec with Matchers
    with JobTestScioContext
    with TollJobFixtures
    with TollBatchJobIo {

  "Toll job" should "run in the batch mode" in {
    JobTest[TollBatchJob.type]
      .args(
        "--effectiveDate=2023-09-15",
        "--entryTable=toll.entry",
        "--exitTable=toll.exit",
        "--entryStatsHourlyTable=toll.entry_stats_hourly",
        "--entryStatsDailyTable=toll.entry_stats_daily",
        "--totalVehicleTimeDailyTable=toll.total_vehicle_time_daily"
      )
      // read toll booth entries and toll booth exists
      .input(CustomIO[TollBoothEntry.Record](EntryTableIoId.id), Seq())
      .input(CustomIO[TollBoothEntry.Record](ExitTableIoId.id), Seq())
      // calculate tool booth stats
      .output(CustomIO[TollBoothStats.Record](EntryStatsHourlyTableIoId.id)) { results =>
        results.debug()
        // TODO
        // results should containSingleValue(anyTollBoothStatsRaw)
      }
      .output(CustomIO[TollBoothStats.Record](EntryStatsDailyTableIoId.id)) { results =>
        results.debug()
        // TODO
        // results should containSingleValue(anyTollBoothStatsRaw)
      }
      // calculate total vehicle times
      .output(CustomIO[TotalVehicleTime.Record](TotalVehicleTimeDailyTableIoId.id)) { results =>
        results.debug()
        // TODO
        // results should containSingleValue(anyTotalVehicleTimeRaw)
      }
      .run()
  }
}
