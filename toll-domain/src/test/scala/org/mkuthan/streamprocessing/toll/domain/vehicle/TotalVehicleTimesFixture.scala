package org.mkuthan.streamprocessing.toll.domain.vehicle

import org.joda.time.Duration
import org.joda.time.Instant

import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothId
import org.mkuthan.streamprocessing.toll.domain.common.LicensePlate

trait TotalVehicleTimesFixture {

  final val anyTotalVehicleTimes: TotalVehicleTimes = TotalVehicleTimes(
    tollBoothId = TollBoothId("1"),
    licensePlate = LicensePlate("JNB 7001"),
    entryTime = Instant.parse("2014-09-10T12:01:00.000Z"),
    exitTime = Instant.parse("2014-09-10T12:03:00.000Z"),
    duration = Duration.standardSeconds(120)
  )

  final val anyTotalVehicleTimesRecord: TotalVehicleTimes.Record = TotalVehicleTimes.Record(
    created_at = Instant.EPOCH,
    toll_booth_id = anyTotalVehicleTimes.tollBoothId.id,
    license_plate = anyTotalVehicleTimes.licensePlate.number,
    entry_time = anyTotalVehicleTimes.entryTime,
    exit_time = anyTotalVehicleTimes.exitTime,
    duration_seconds = anyTotalVehicleTimes.duration.getStandardSeconds
  )
}
