package org.mkuthan.streamprocessing.toll.domain.toll

import com.spotify.scio.bigquery.types.BigQueryType
import com.spotify.scio.coders.Coder
import com.spotify.scio.values.SCollection

import org.joda.time.Duration
import org.joda.time.Instant

import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothEntry
import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothExit
import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothId
import org.mkuthan.streamprocessing.toll.domain.common.LicensePlate
import org.mkuthan.streamprocessing.toll.domain.diagnostic.Diagnostic

final case class TotalCarTime(
    licencePlate: LicensePlate,
    tollBoothId: TollBoothId,
    entryTime: Instant,
    exitTime: Instant,
    duration: Duration
)

object TotalCarTime {

  // implicit val CoderCache: Coder[TotalCarTime] = Coder.gen

  @BigQueryType.toTable
  final case class Raw(
      licence_plate: String,
      toll_booth_id: String,
      entryTime: Instant,
      exitTime: Instant,
      duration_seconds: Int
  )

  def calculate(
      boothEntries: SCollection[TollBoothEntry],
      boothExits: SCollection[TollBoothExit]
  ): (SCollection[TotalCarTime], SCollection[Diagnostic]) = ???

  def encode(input: SCollection[TotalCarTime]): SCollection[Raw] = ???

}
