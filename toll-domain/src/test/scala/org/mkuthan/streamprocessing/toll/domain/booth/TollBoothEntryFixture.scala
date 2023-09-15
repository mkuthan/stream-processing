package org.mkuthan.streamprocessing.toll.domain.booth

import org.joda.time.Instant

import org.mkuthan.streamprocessing.shared.common.DeadLetter
import org.mkuthan.streamprocessing.toll.domain.common.LicensePlate

trait TollBoothEntryFixture {

  final val anyTollBoothEntryPayload = TollBoothEntry.Payload(
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

  final val tollBoothEntryPayloadInvalid = anyTollBoothEntryPayload.copy(entry_time = "invalid time")

  final val tollBoothEntryDecodingError = DeadLetter[TollBoothEntry.Payload](
    data = tollBoothEntryPayloadInvalid,
    error = "Invalid format: \"invalid time\""
  )

  final val tollBoothEntryPayloadWithoutExit = anyTollBoothEntryPayload.copy(license_plate = "other license plate")

  final val anyTollBoothEntry = TollBoothEntry(
    id = TollBoothId("1"),
    entryTime = Instant.parse("2014-09-10T12:01:00Z"),
    toll = BigDecimal(7),
    licensePlate = LicensePlate("JNB 7001")
  )
}
