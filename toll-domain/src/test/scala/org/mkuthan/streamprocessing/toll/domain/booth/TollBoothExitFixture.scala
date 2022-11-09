package org.mkuthan.streamprocessing.toll.domain.booth

import org.joda.time.Instant

import org.mkuthan.streamprocessing.toll.domain.common.LicensePlate

trait TollBoothExitFixture {

  val anyTollBoothExitRaw = TollBoothExit.Raw(
    id = "1",
    exit_time = "2014-09-10T12:03:00Z",
    license_plate = "JNB 7001"
  )

  val tollBoothExitRawInvalid = anyTollBoothExitRaw.copy(exit_time = "invalid time")

  val anyTollBoothExit = TollBoothExit(
    id = TollBoothId("1"),
    exitTime = Instant.parse("2014-09-10T12:03:00Z"),
    licensePlate = LicensePlate("JNB 7001")
  )
}
