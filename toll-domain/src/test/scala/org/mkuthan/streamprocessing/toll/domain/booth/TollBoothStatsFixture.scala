package org.mkuthan.streamprocessing.toll.domain.booth

import org.joda.time.Instant

trait TollBoothStatsFixture {

  val anyTollBoothStats = TollBoothStats(
    id = TollBoothId("1"),
    totalToll = BigDecimal(7),
    count = 1,
    firstEntryTime = Instant.parse("2014-09-10T12:01:00.000Z"),
    lastEntryTime = Instant.parse("2014-09-10T12:01:00.000Z")
  )

  val anyTollBoothStatsRaw = TollBoothStats.Raw(
    record_timestamp = Instant.parse("2014-09-10T12:09:59.999Z"), // end of fixed window
    id = "1",
    total_toll = BigDecimal(7),
    count = 1,
    first_entry_time = Instant.parse("2014-09-10T12:01:00.000Z"),
    last_entry_time = Instant.parse("2014-09-10T12:01:00.000Z")
  )
}
