package org.mkuthan.streamprocessing.toll.domain.booth

import com.spotify.scio.values.SCollection

import org.joda.time.Instant

import org.mkuthan.streamprocessing.toll.domain.common.LicensePlate

final case class TollBoothExit(
    id: TollBoothId,
    exitTime: Instant,
    licensePlate: LicensePlate
)

object TollBoothExit {

  // implicit val CoderCache: Coder[TollBoothExit] = Coder.gen

  final case class Raw(
      id: String,
      exit_time: String,
      license_plate: String
  )

  def decode(raw: SCollection[Raw]): (SCollection[TollBoothExit], SCollection[Raw]) =
    (raw.context.empty[TollBoothExit](), raw.context.empty[Raw]())
}
