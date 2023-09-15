package org.mkuthan.streamprocessing.toll.application.batch

import org.mkuthan.streamprocessing.infrastructure.common.IoIdentifier
import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothEntry
import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothExit

trait TollBatchJobIo extends TollBoothIo

trait TollBoothIo {
  val EntryTableIoId: IoIdentifier[TollBoothEntry.Record] =
    IoIdentifier[TollBoothEntry.Record]("entry-table-id")

  val ExitTableIoId: IoIdentifier[TollBoothExit.Record] =
    IoIdentifier[TollBoothExit.Record]("exit-table-id")
}
