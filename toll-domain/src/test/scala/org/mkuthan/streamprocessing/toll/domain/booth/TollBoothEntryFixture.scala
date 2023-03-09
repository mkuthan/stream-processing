package org.mkuthan.streamprocessing.toll.domain.booth

import org.joda.time.Instant

import org.mkuthan.streamprocessing.toll.domain.common.DeadLetter
import org.mkuthan.streamprocessing.toll.domain.common.LicensePlate

trait TollBoothEntryFixture {

  val anyTollBoothEntryRaw = TollBoothEntry.Raw(
    id = "1",
    entry_time = "2014-09-10T12:01:00Z",
    license_plate = "JNB 7001",
    state = "NY",
    make = "Honda",
    model = "CRV",
    vehicle_type = "1",
    weight_type = "0",
    toll = "7",
    tag = "String"
  )

  val tollBoothEntryRawInvalid = anyTollBoothEntryRaw.copy(entry_time = "invalid time")

  val tollBoothEntryDecodingError = DeadLetter[TollBoothEntry.Raw](
    data = tollBoothEntryRawInvalid,
    error = "Invalid format: \"invalid time\""
  )

  val tollBoothEntryRawWithoutExit = anyTollBoothEntryRaw.copy(license_plate = "other license plate")

  val anyTollBoothEntry = TollBoothEntry(
    id = TollBoothId("1"),
    entryTime = Instant.parse("2014-09-10T12:01:00Z"),
    toll = BigDecimal(7),
    licensePlate = LicensePlate("JNB 7001")
  )
}
