package org.mkuthan.streamprocessing.toll.domain.toll

import com.spotify.scio.bigquery.types.BigQueryType

import org.joda.time.Duration
import org.joda.time.Instant

import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothId
import org.mkuthan.streamprocessing.toll.domain.common.LicensePlate

trait TotalCarTimeFixture {
  val anyTotalCarTime = TotalCarTime(
    tollBoothId = TollBoothId("1"),
    licensePlate = LicensePlate("JNB 7001"),
    entryTime = Instant.parse("2014-09-10T12:01:00.000Z"),
    exitTime = Instant.parse("2014-09-10T12:03:00.000Z"),
    duration = Duration.standardSeconds(120)
  )

  val anyTotalCarTimeRaw = TotalCarTime.Raw(
    record_timestamp = Instant.parse("2014-09-10T12:12:59.999Z"), // end of session window
    toll_booth_id = "1",
    license_plate = "JNB 7001",
    entry_time = Instant.parse("2014-09-10T12:01:00.000Z"),
    exit_time = Instant.parse("2014-09-10T12:03:00.000Z"),
    duration_seconds = 120L
  )

  private val bigQueryType = BigQueryType[TotalCarTime.Raw]
  val anyTotalCarTimeRawTableRow = bigQueryType.toTableRow(anyTotalCarTimeRaw)
}
