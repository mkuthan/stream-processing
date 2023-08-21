package org.mkuthan.streamprocessing.toll.domain.booth

import com.spotify.scio.bigquery.types.BigQueryType
import com.spotify.scio.values.SCollection

import com.twitter.algebird.Semigroup
import org.joda.time.Duration
import org.joda.time.Instant

case class TollBoothStats(
    id: TollBoothId,
    count: Int,
    totalToll: BigDecimal,
    firstEntryTime: Instant,
    lastEntryTime: Instant
) {
  def before(other: TollBoothStats): Boolean =
    firstEntryTime.isBefore(other.firstEntryTime)
  def after(other: TollBoothStats): Boolean =
    lastEntryTime.isAfter(other.lastEntryTime)
}

object TollBoothStats {

  @BigQueryType.toTable
  case class Raw(
      record_timestamp: Instant,
      id: String,
      count: Int,
      total_toll: BigDecimal,
      first_entry_time: Instant,
      last_entry_time: Instant
  )

  private object TollBoothStatsSemigroup extends Semigroup[TollBoothStats] {
    override def plus(x: TollBoothStats, y: TollBoothStats): TollBoothStats = {
      require(x.id == y.id)

      TollBoothStats(
        id = x.id,
        count = x.count + y.count,
        totalToll = x.totalToll + y.totalToll,
        firstEntryTime = if (x.before(y)) x.firstEntryTime else y.firstEntryTime,
        lastEntryTime = if (x.after(y)) x.lastEntryTime else y.lastEntryTime
      )
    }
  }

  def calculateInFixedWindow(input: SCollection[TollBoothEntry], duration: Duration): SCollection[TollBoothStats] =
    input
      .keyBy(_.id)
      .mapValues(fromBoothEntry)
      .withFixedWindows(duration)
      .sumByKey(TollBoothStatsSemigroup)
      .values

  def encode(input: SCollection[TollBoothStats]): SCollection[Raw] =
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

  private def fromBoothEntry(boothEntry: TollBoothEntry): TollBoothStats = TollBoothStats(
    id = boothEntry.id,
    count = 1,
    totalToll = boothEntry.toll,
    firstEntryTime = boothEntry.entryTime,
    lastEntryTime = boothEntry.entryTime
  )
}
