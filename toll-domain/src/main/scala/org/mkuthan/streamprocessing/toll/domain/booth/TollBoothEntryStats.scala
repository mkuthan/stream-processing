package org.mkuthan.streamprocessing.toll.domain.booth

import com.spotify.scio.bigquery.types.BigQueryType
import com.spotify.scio.values.SCollection

import org.joda.time.Duration
import org.joda.time.Instant

final case class TollBoothEntryStats(
    id: TollBoothId,
    beginTime: Instant,
    endTime: Instant,
    count: Int,
    totalToll: BigDecimal
)

object TollBoothEntryStats {

  // implicit val CoderCache: Coder[TollBoothEntryStats] = Coder.gen

  @BigQueryType.toTable
  final case class Raw(
      id: String,
      begin_time: Instant,
      end_time: Instant,
      count: Int,
      total_toll: String
  )

  def calculateInFixedWindow(input: SCollection[TollBoothEntry], duration: Duration): SCollection[TollBoothEntryStats] =
    input.context.empty[TollBoothEntryStats]()

  def encode(input: SCollection[TollBoothEntryStats]): SCollection[Raw] =
    input.context.empty[Raw]()
}