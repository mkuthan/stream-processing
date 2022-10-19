package org.mkuthan.streamprocessing.toll.domain.toll

import org.joda.time.{Duration, Instant}
import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothId
import org.mkuthan.streamprocessing.toll.domain.common.LicensePlate

case class TotalCarTime(
    licencePlate: LicensePlate,
    tollBoothId: TollBoothId,
    entryTime: Instant,
    exitTime: Instant,
    duration: Duration
)
