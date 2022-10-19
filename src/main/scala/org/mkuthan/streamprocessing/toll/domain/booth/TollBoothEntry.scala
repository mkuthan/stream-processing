package org.mkuthan.streamprocessing.toll.domain.booth

import com.spotify.scio.values.SCollection
import org.joda.time.{Duration, Instant}
import org.mkuthan.streamprocessing.toll.configuration.PubSubSubscription
import org.mkuthan.streamprocessing.toll.domain.common.LicensePlate

case class TollBoothEntry(
    id: TollBoothId,
    entryTime: Instant,
    licencePlate: LicensePlate,
    toll: BigDecimal
)

object TollBoothEntry {
  def decode(raw: SCollection[TollBoothEntryRaw]): (SCollection[TollBoothEntry], SCollection[TollBoothEntryRaw]) = ???
}
