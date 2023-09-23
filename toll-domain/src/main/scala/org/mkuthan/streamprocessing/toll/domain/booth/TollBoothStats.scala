package org.mkuthan.streamprocessing.toll.domain.booth

import com.spotify.scio.bigquery.types.BigQueryType
import com.spotify.scio.values.SCollection
import com.spotify.scio.values.WindowOptions

import org.joda.time.Duration
import org.joda.time.Instant

import org.mkuthan.streamprocessing.shared.scio.syntax._
import org.mkuthan.streamprocessing.shared.scio.SumByKey

case class TollBoothStats(
    id: TollBoothId,
    count: Int,
    totalToll: BigDecimal,
    firstEntryTime: Instant,
    lastEntryTime: Instant
) {
  private def before(other: TollBoothStats): Boolean =
    firstEntryTime.isBefore(other.firstEntryTime)
  private def after(other: TollBoothStats): Boolean =
    lastEntryTime.isAfter(other.lastEntryTime)
}

object TollBoothStats {

  @BigQueryType.toTable
  case class Record(
      created_at: Instant,
      id: String,
      count: Int,
      total_toll: BigDecimal,
      first_entry_time: Instant,
      last_entry_time: Instant
  )

  implicit val TollBoothStatsSumByKey: SumByKey[TollBoothStats] = SumByKey.create(
    keyFn = _.id.id,
    plusFn = (x, y) =>
      x.copy(
        count = x.count + y.count,
        totalToll = x.totalToll + y.totalToll,
        firstEntryTime = if (x.before(y)) x.firstEntryTime else y.firstEntryTime,
        lastEntryTime = if (x.after(y)) x.lastEntryTime else y.lastEntryTime
      )
  )

  def calculateInFixedWindow(
      input: SCollection[TollBoothEntry],
      windowDuration: Duration,
      windowOptions: WindowOptions
  ): SCollection[TollBoothStats] =
    input
      .map(fromBoothEntry)
      .sumByKeyInFixedWindow(windowDuration = windowDuration, windowOptions = windowOptions)

  def encode(input: SCollection[TollBoothStats]): SCollection[Record] =
    input.mapWithTimestamp { case (r, t) =>
      Record(
        created_at = t,
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
