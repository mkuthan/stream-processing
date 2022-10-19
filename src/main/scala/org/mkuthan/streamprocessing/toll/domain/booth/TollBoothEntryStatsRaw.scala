package org.mkuthan.streamprocessing.toll.domain.booth

import com.spotify.scio.bigquery.types.BigQueryType
import org.joda.time.Instant

@BigQueryType.toTable
case class TollBoothEntryStatsRaw(
    id: String,
    begin_time: Instant,
    end_time: Instant,
    count: Int,
    totalToll: String
)
