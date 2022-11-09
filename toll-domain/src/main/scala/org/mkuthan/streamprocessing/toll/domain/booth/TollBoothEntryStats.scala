package org.mkuthan.streamprocessing.toll.domain.booth

import com.spotify.scio.bigquery.types.BigQueryType
import com.spotify.scio.coders.Coder
import com.spotify.scio.values.SCollection

import com.twitter.algebird.Semigroup
import org.joda.time.Duration
import org.joda.time.Instant

final case class TollBoothEntryStats(
    id: TollBoothId,
    count: Int,
    totalToll: BigDecimal,
    firstEntryTime: Instant,
    lastEntryTime: Instant
) {
  def before(other: TollBoothEntryStats): Boolean =
    firstEntryTime.isBefore(other.firstEntryTime)
  def after(other: TollBoothEntryStats): Boolean =
    lastEntryTime.isAfter(other.lastEntryTime)
}

object TollBoothEntryStats {

  implicit val CoderCache: Coder[TollBoothEntryStats] = Coder.gen
  implicit val CoderCacheRaw: Coder[TollBoothEntryStats.Raw] = Coder.gen

  @BigQueryType.toTable
  final case class Raw(
      record_timestamp: Instant,
      id: String,
      count: Int,
      total_toll: BigDecimal,
      first_entry_time: Instant,
      last_entry_time: Instant
  )

  object TollBoothEntrySemigroup extends Semigroup[TollBoothEntryStats] {
    override def plus(x: TollBoothEntryStats, y: TollBoothEntryStats): TollBoothEntryStats = {
      require(x.id == y.id)

      TollBoothEntryStats(
        id = x.id,
        count = x.count + y.count,
        totalToll = x.totalToll + y.totalToll,
        firstEntryTime = if (x.before(y)) x.firstEntryTime else y.firstEntryTime,
        lastEntryTime = if (x.after(y)) x.lastEntryTime else y.lastEntryTime
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
    input.withTimestamp.map { case (r, t) =>
      Raw(
        record_timestamp = t,
        id = r.id.id,
        count = r.count,
        total_toll = r.totalToll,
        first_entry_time = r.firstEntryTime,
        last_entry_time = r.lastEntryTime
      )
    }

  def fromBoothEntry(boothEntry: TollBoothEntry): TollBoothEntryStats = TollBoothEntryStats(
    id = boothEntry.id,
    count = 1,
    totalToll = boothEntry.toll,
    firstEntryTime = boothEntry.entryTime,
    lastEntryTime = boothEntry.entryTime
  )
}
