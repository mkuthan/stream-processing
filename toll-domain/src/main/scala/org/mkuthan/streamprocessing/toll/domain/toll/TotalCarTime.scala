package org.mkuthan.streamprocessing.toll.domain.toll

import com.spotify.scio.bigquery.types.BigQueryType
import com.spotify.scio.coders.Coder
import com.spotify.scio.values.SCollection
import com.spotify.scio.values.SideOutput

import org.joda.time.Duration
import org.joda.time.Instant

import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothEntry
import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothExit
import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothId
import org.mkuthan.streamprocessing.toll.domain.common.LicensePlate
import org.mkuthan.streamprocessing.toll.domain.diagnostic.Diagnostic
import org.mkuthan.streamprocessing.toll.domain.diagnostic.MissingTollBoothExit

final case class TotalCarTime(
    licencePlate: LicensePlate,
    tollBoothId: TollBoothId,
    entryTime: Instant,
    exitTime: Instant,
    duration: Duration
)

object TotalCarTime {

  implicit val CoderCache: Coder[TotalCarTime] = Coder.gen
  implicit val CoderCacheRaw: Coder[TotalCarTime.Raw] = Coder.gen

  @BigQueryType.toTable
  final case class Raw(
      licence_plate: String,
      toll_booth_id: String,
      entry_time: Instant,
      exit_time: Instant,
      duration_seconds: Int
  )

  def calculateInSessionWindow(
      boothEntries: SCollection[TollBoothEntry],
      boothExits: SCollection[TollBoothExit],
      gapDuration: Duration
  ): (SCollection[TotalCarTime], SCollection[Diagnostic]) = {
    val boothEntriesById = boothEntries
      .keyBy(entry => (entry.id, entry.licensePlate))
      .withSessionWindows(gapDuration)
    val boothExistsById = boothExits
      .keyBy(exit => (exit.id, exit.licensePlate))
      .withSessionWindows(gapDuration)

    val diagnostic = SideOutput[Diagnostic]()
    val (results, sideOutputs) = boothEntriesById
      .leftOuterJoin(boothExistsById)
      .values
      .withSideOutputs(diagnostic)
      .flatMap {
        case ((boothEntry, Some(boothExit)), _) =>
          Some(totalCarTime(boothEntry, boothExit))
        case ((boothEntry, None), ctx) =>
          ctx.output(diagnostic, toDiagnostic(boothEntry))
          None
      }
    (results, sideOutputs(diagnostic))
  }

  def encode(input: SCollection[TotalCarTime]): SCollection[Raw] =
    input.context.empty[Raw]()

  private def totalCarTime(boothEntry: TollBoothEntry, boothExit: TollBoothExit): TotalCarTime = {
    val diff = boothExit.exitTime.getMillis - boothEntry.entryTime.getMillis
    TotalCarTime(
      licencePlate = boothEntry.licensePlate,
      tollBoothId = boothEntry.id,
      entryTime = boothEntry.entryTime,
      exitTime = boothExit.exitTime,
      duration = Duration.millis(diff)
    )
  }

  private def toDiagnostic(boothEntry: TollBoothEntry): Diagnostic =
    Diagnostic(
      boothId = boothEntry.id,
      reason = MissingTollBoothExit
    )
}
