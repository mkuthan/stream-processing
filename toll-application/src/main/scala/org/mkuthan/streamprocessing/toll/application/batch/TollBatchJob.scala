package org.mkuthan.streamprocessing.toll.application.batch

import com.spotify.scio.ContextAndArgs

import org.mkuthan.streamprocessing.infrastructure._
import org.mkuthan.streamprocessing.infrastructure.bigquery.RowRestriction
import org.mkuthan.streamprocessing.infrastructure.bigquery.StorageReadConfiguration

object TollBatchJob extends TollBatchJobIo {
  def main(mainArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(mainArgs)

    val config = TollBatchJobConfig.parse(args)

    val boothEntryRecords = sc.readFromBigQuery(
      EntryTableIoId,
      config.entryTable,
      StorageReadConfiguration()
        .withRowRestriction(
          RowRestriction.SqlRestriction(s"TIMESTAMP_TRUNC(entry_time, DAY) = '${config.effectiveDate}'")
        )
    )

    val boothExitRecords = sc.readFromBigQuery(
      ExitTableIoId,
      config.exitTable,
      StorageReadConfiguration()
        .withRowRestriction(
          RowRestriction.SqlRestriction(s"TIMESTAMP_TRUNC(exit_time, DAY) = '${config.effectiveDate}'")
        )
    )

    val _ = sc.run()
  }
}
