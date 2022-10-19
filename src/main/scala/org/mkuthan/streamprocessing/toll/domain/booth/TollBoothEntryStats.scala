package org.mkuthan.streamprocessing.toll.domain.booth

import com.spotify.scio.values.SCollection
import org.joda.time.{Duration, Instant}

case class TollBothEntryStats(
    id: TollBoothId,
    beginTime: Instant,
    endTime: Instant,
    count: Int,
    totalToll: BigDecimal
)

object TollBoothEntryStats {
  def calculateInFixedWindow(entries: SCollection[TollBoothEntry], duration: Duration): SCollection[TollBothEntryStats] = ???
  def saveStats(counts: SCollection[TollBothEntryStats]): Unit = ???
}
