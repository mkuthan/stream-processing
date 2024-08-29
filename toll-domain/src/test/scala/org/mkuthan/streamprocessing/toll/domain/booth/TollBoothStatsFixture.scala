package org.mkuthan.streamprocessing.toll.domain.booth

import org.joda.time.Instant

trait TollBoothStatsFixture {

  final val anyTollBoothStats: TollBoothStats = TollBoothStats(
    id = TollBoothId("1"),
    totalToll = BigDecimal(7),
    count = 1,
    firstEntryTime = Instant.parse("2014-09-10T12:01:00.000Z"),
    lastEntryTime = Instant.parse("2014-09-10T12:01:00.000Z")
  )

  final val anyTollBoothStatsRecord: TollBoothStats.Record = TollBoothStats.Record(
    created_at = Instant.EPOCH,
    id = anyTollBoothStats.id.id,
    total_toll = anyTollBoothStats.totalToll,
    count = anyTollBoothStats.count,
    first_entry_time = anyTollBoothStats.firstEntryTime,
    last_entry_time = anyTollBoothStats.lastEntryTime
  )
}

object TollBoothStatsFixture extends TollBoothEntryFixture
