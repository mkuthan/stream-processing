package org.mkuthan.streamprocessing.toll.domain.booth

import org.joda.time.Instant

import org.mkuthan.streamprocessing.shared.common.DeadLetter
import org.mkuthan.streamprocessing.shared.common.Message
import org.mkuthan.streamprocessing.toll.domain.common.LicensePlate

trait TollBoothEntryFixture {

  private final val anyTollBoothEntryPayload: TollBoothEntry.Payload = TollBoothEntry.Payload(
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

  final val anyTollBoothEntryMessage: Message[TollBoothEntry.Payload] = Message(
    anyTollBoothEntryPayload,
    Map(TollBoothEntry.TimestampAttribute -> anyTollBoothEntryPayload.entry_time)
  )

  final val invalidTollBoothEntryMessage: Message[TollBoothEntry.Payload] = anyTollBoothEntryMessage.copy(
    payload = anyTollBoothEntryPayload.copy(id = "")
  )

  final val tollBoothEntryDecodingError: DeadLetter[TollBoothEntry.Payload] = DeadLetter[TollBoothEntry.Payload](
    data = invalidTollBoothEntryMessage.payload,
    error = "requirement failed: Toll booth id is empty"
  )

  final val anyTollBoothEntryRecord: TollBoothEntry.Record = TollBoothEntry.Record(
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

  final val anyTollBoothEntry: TollBoothEntry = TollBoothEntry(
    id = TollBoothId(anyTollBoothEntryPayload.id),
    entryTime = Instant.parse(anyTollBoothEntryPayload.entry_time),
    toll = BigDecimal(anyTollBoothEntryPayload.toll),
    licensePlate = LicensePlate(anyTollBoothEntryPayload.license_plate)
  )
}

object TollBoothEntryFixture extends TollBoothEntryFixture
