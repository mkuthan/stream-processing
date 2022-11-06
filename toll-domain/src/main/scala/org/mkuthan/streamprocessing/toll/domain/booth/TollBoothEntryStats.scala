package org.mkuthan.streamprocessing.toll.domain.booth

import com.spotify.scio.bigquery.types.BigQueryType
import com.spotify.scio.coders.Coder
import com.spotify.scio.values.SCollection

import com.twitter.algebird.Semigroup
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

  implicit val CoderCache: Coder[TollBoothEntryStats] = Coder.gen
  implicit val CoderCacheRaw: Coder[TollBoothEntryStats.Raw] = Coder.gen

  @BigQueryType.toTable
  final case class Raw(
      id: String,
      begin_time: Instant,
      end_time: Instant,
      count: Int,
      total_toll: String
  )

  object TollBoothEntrySemigroup extends Semigroup[TollBoothEntryStats] {
    override def plus(x: TollBoothEntryStats, y: TollBoothEntryStats): TollBoothEntryStats = {
      require(x.id == y.id)

      TollBoothEntryStats(
        id = x.id,
        beginTime = if (x.beginTime.isBefore(y.beginTime)) x.beginTime else y.beginTime,
        endTime = if (x.endTime.isAfter(y.endTime)) x.endTime else y.endTime,
        count = x.count + y.count,
        totalToll = x.totalToll + y.totalToll
      )
    }
  }

  def calculateInFixedWindow(input: SCollection[TollBoothEntry], duration: Duration): SCollection[TollBoothEntryStats] =
    input
      .keyBy(_.id)
      .mapValues(fromBoothEntry)
      .withFixedWindows(duration)
      .sumByKey(TollBoothEntrySemigroup)
      .values

  def encode(input: SCollection[TollBoothEntryStats]): SCollection[Raw] =
    input.context.empty[Raw]()

  def fromBoothEntry(boothEntry: TollBoothEntry): TollBoothEntryStats = TollBoothEntryStats(
    id = boothEntry.id,
    beginTime = boothEntry.entryTime,
    endTime = boothEntry.entryTime,
    count = 1,
    totalToll = boothEntry.toll
  )
}
