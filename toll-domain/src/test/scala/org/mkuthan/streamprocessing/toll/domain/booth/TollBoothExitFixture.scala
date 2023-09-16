package org.mkuthan.streamprocessing.toll.domain.booth

import org.joda.time.Instant

import org.mkuthan.streamprocessing.shared.common.DeadLetter
import org.mkuthan.streamprocessing.toll.domain.common.LicensePlate

trait TollBoothExitFixture {

  final val anyTollBoothExitPayload = TollBoothExit.Payload(
    id = "1",
    exit_time = "2014-09-10T12:03:00Z",
    license_plate = "JNB 7001"
  )

  final val tollBoothExitPayloadInvalid = anyTollBoothExitPayload.copy(exit_time = "invalid time")

  final val tollBoothExitDecodingError = DeadLetter[TollBoothExit.Payload](
    data = tollBoothExitPayloadInvalid,
    error = "Invalid format: \"invalid time\""
  )

  final val anyTollBoothExitRecord = TollBoothExit.Record(
    id = anyTollBoothExitPayload.id,
    exit_time = Instant.parse(anyTollBoothExitPayload.exit_time),
    license_plate = anyTollBoothExitPayload.license_plate
  )

  final val anyTollBoothExit = TollBoothExit(
    id = TollBoothId(anyTollBoothExitPayload.id),
    exitTime = Instant.parse(anyTollBoothExitPayload.exit_time),
    licensePlate = LicensePlate(anyTollBoothExitPayload.license_plate)
  )
}
