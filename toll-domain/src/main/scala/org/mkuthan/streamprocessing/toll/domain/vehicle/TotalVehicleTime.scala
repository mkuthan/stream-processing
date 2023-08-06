package org.mkuthan.streamprocessing.toll.domain.vehicle

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

final case class TotalVehicleTime(
    licensePlate: LicensePlate,
    tollBoothId: TollBoothId,
    entryTime: Instant,
    exitTime: Instant,
    duration: Duration
)

object TotalVehicleTime {

  implicit val CoderCache: Coder[TotalVehicleTime] = Coder.gen
  implicit val CoderCacheRaw: Coder[TotalVehicleTime.Raw] = Coder.gen

  @BigQueryType.toTable
  final case class Raw(
      record_timestamp: Instant,
      license_plate: String,
      toll_booth_id: String,
      entry_time: Instant,
      exit_time: Instant,
      duration_seconds: Long
  )

  def calculateInSessionWindow(
      boothEntries: SCollection[TollBoothEntry],
      boothExits: SCollection[TollBoothExit],
      gapDuration: Duration
  ): (SCollection[TotalVehicleTime], SCollection[Diagnostic]) = {
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
          Some(totalVehicleTime(boothEntry, boothExit))
        case ((boothEntry, None), ctx) =>
          ctx.output(diagnostic, toDiagnostic(boothEntry))
          None
      }
    (results, sideOutputs(diagnostic))
  }

  def encode(input: SCollection[TotalVehicleTime]): SCollection[Raw] =
    input.withTimestamp.map { case (r, t) =>
      Raw(
        record_timestamp = t,
        license_plate = r.licensePlate.number,
        toll_booth_id = r.tollBoothId.id,
        entry_time = r.entryTime,
        exit_time = r.exitTime,
        duration_seconds = r.duration.getStandardSeconds
      )
    }

  private def totalVehicleTime(boothEntry: TollBoothEntry, boothExit: TollBoothExit): TotalVehicleTime = {
    val diff = boothExit.exitTime.getMillis - boothEntry.entryTime.getMillis
    TotalVehicleTime(
      licensePlate = boothEntry.licensePlate,
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