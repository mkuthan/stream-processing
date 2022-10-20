package org.mkuthan.streamprocessing.toll.domain.booth

import com.spotify.scio.bigquery.types.BigQueryType
import com.spotify.scio.values.SCollection
import org.joda.time.{Duration, Instant}

final case class TollBoothEntryStats(
    id: TollBoothId,
    beginTime: Instant,
    endTime: Instant,
    count: Int,
    totalToll: BigDecimal
)

object TollBoothEntryStats {
  @BigQueryType.toTable
  final case class Raw(
      id: String,
      begin_time: Instant,
      end_time: Instant,
      count: Int,
      totalToll: String
  )

  def calculateInFixedWindow(input: SCollection[TollBoothEntry], duration: Duration): SCollection[TollBoothEntryStats] = ???

  def encode(input: SCollection[TollBoothEntryStats]): SCollection[Raw] = ???
}
