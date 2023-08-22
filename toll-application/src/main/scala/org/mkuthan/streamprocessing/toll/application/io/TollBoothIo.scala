package org.mkuthan.streamprocessing.toll.application.io

import org.mkuthan.streamprocessing.infrastructure.common.IoIdentifier
import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothEntry
import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothExit
import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothStats

trait TollBoothIo {
  val EntrySubscriptionIoId: IoIdentifier[TollBoothEntry.Raw] =
    IoIdentifier[TollBoothEntry.Raw]("entry-subscription-id")
  val EntryDlqBucketIoId: IoIdentifier[TollBoothEntry.DeadLetterRaw] =
    IoIdentifier[TollBoothEntry.DeadLetterRaw]("entry-dlq-bucket-id")

  val ExitSubscriptionIoId: IoIdentifier[TollBoothExit.Raw] =
    IoIdentifier[TollBoothExit.Raw]("exit-subscription-id")
  val ExitDlqBucketIoId: IoIdentifier[TollBoothExit.DeadLetterRaw] =
    IoIdentifier[TollBoothExit.DeadLetterRaw]("exit-dlq-bucket-id")

  val EntryStatsTableIoId: IoIdentifier[TollBoothStats.Raw] =
    IoIdentifier[TollBoothStats.Raw]("entry-stats-table-id")

}
