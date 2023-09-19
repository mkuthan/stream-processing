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

  final val tollBoothEntryPayloadInvalid = anyTollBoothEntryPayload.copy(id = "")

  final val tollBoothEntryDecodingError = DeadLetter[TollBoothEntry.Payload](
    data = tollBoothEntryPayloadInvalid,
    error = "requirement failed: Toll booth id is empty"
  )

  final val tollBoothEntryPayloadWithoutExit = anyTollBoothEntryPayload.copy(license_plate = "other license plate")

  final val anyTollBoothEntryRecord = TollBoothEntry.Record(
    id = anyTollBoothEntryPayload.id,
    entry_time = Instant.parse(anyTollBoothEntryPayload.entry_time),
    license_plate = anyTollBoothEntryPayload.license_plate,
    state = anyTollBoothEntryPayload.state,
    make = anyTollBoothEntryPayload.make,
    model = anyTollBoothEntryPayload.model,
    vehicle_type = anyTollBoothEntryPayload.vehicle_type,
    weight_type = anyTollBoothEntryPayload.weight_type,
    toll = BigDecimal(anyTollBoothEntryPayload.toll),
    tag = anyTollBoothEntryPayload.tag
  )

  final val anyTollBoothEntry = TollBoothEntry(
    id = TollBoothId(anyTollBoothEntryPayload.id),
    entryTime = Instant.parse(anyTollBoothEntryPayload.entry_time),
    toll = BigDecimal(anyTollBoothEntryPayload.toll),
    licensePlate = LicensePlate(anyTollBoothEntryPayload.license_plate)
  )
}
