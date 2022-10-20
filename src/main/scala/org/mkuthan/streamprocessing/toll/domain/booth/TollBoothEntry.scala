package org.mkuthan.streamprocessing.toll.domain.booth

import com.spotify.scio.values.SCollection
import org.joda.time.Instant
import org.mkuthan.streamprocessing.toll.domain.common.LicensePlate

final case class TollBoothEntry(
    id: TollBoothId,
    entryTime: Instant,
    licencePlate: LicensePlate,
    toll: BigDecimal
)

object TollBoothEntry {
  final case class Raw(
      id: String,
      entry_time: String,
      license_plate: String,
      state: String,
      make: String,
      model: String,
      vehicle_type: String,
      weight_type: String,
      toll: String,
      tag: String
  )

  def decode(raw: SCollection[Raw]): (SCollection[TollBoothEntry], SCollection[Raw]) = ???
}
