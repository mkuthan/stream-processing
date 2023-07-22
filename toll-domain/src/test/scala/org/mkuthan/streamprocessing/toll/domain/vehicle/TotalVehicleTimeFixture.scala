package org.mkuthan.streamprocessing.toll.domain.vehicle

import org.joda.time.Duration
import org.joda.time.Instant

import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothId
import org.mkuthan.streamprocessing.toll.domain.common.LicensePlate

trait TotalVehicleTimeFixture {
  val anyTotalVehicleTime = TotalVehicleTime(
    tollBoothId = TollBoothId("1"),
    licensePlate = LicensePlate("JNB 7001"),
    entryTime = Instant.parse("2014-09-10T12:01:00.000Z"),
    exitTime = Instant.parse("2014-09-10T12:03:00.000Z"),
    duration = Duration.standardSeconds(120)
  )

  val anyTotalVehicleTimeRaw = TotalVehicleTime.Raw(
    record_timestamp = Instant.parse("2014-09-10T12:12:59.999Z"), // end of session window
    toll_booth_id = "1",
    license_plate = "JNB 7001",
    entry_time = Instant.parse("2014-09-10T12:01:00.000Z"),
    exit_time = Instant.parse("2014-09-10T12:03:00.000Z"),
    duration_seconds = 120L
  )
}
