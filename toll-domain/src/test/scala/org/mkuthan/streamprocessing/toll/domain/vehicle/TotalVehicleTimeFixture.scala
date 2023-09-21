package org.mkuthan.streamprocessing.toll.domain.vehicle

import org.joda.time.Duration
import org.joda.time.Instant

import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothId
import org.mkuthan.streamprocessing.toll.domain.common.LicensePlate

trait TotalVehicleTimeFixture {

  final val anyTotalVehicleTime: TotalVehicleTime = TotalVehicleTime(
    tollBoothId = TollBoothId("1"),
    licensePlate = LicensePlate("JNB 7001"),
    entryTime = Instant.parse("2014-09-10T12:01:00.000Z"),
    exitTime = Instant.parse("2014-09-10T12:03:00.000Z"),
    duration = Duration.standardSeconds(120)
  )

  final val anyTotalVehicleTimeRecord: TotalVehicleTime.Record = TotalVehicleTime.Record(
    created_at = Instant.EPOCH,
    toll_booth_id = anyTotalVehicleTime.tollBoothId.id,
    license_plate = anyTotalVehicleTime.licensePlate.number,
    entry_time = anyTotalVehicleTime.entryTime,
    exit_time = anyTotalVehicleTime.exitTime,
    duration_seconds = anyTotalVehicleTime.duration.getStandardSeconds
  )
}
