package org.mkuthan.streamprocessing.toll.domain.vehicle

import com.spotify.scio.bigquery.types.BigQueryType
import com.spotify.scio.values.SCollection

import org.joda.time.Duration
import org.joda.time.Instant

import org.mkuthan.streamprocessing.shared.scio.syntax._
import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothEntry
import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothExit
import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothId
import org.mkuthan.streamprocessing.toll.domain.common.LicensePlate

final case class TotalVehicleTime(
    licensePlate: LicensePlate,
    tollBoothId: TollBoothId,
    entryTime: Instant,
    exitTime: Instant,
    duration: Duration
)

object TotalVehicleTime {

  @BigQueryType.toTable
  case class Record(
      created_at: Instant,
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
  ): (SCollection[TotalVehicleTime], SCollection[TotalVehicleTimeDiagnostic]) = {
    val boothEntriesById = boothEntries
      .keyBy(entry => (entry.id, entry.licensePlate))
      .withSessionWindows(gapDuration)
    val boothExistsById = boothExits
      .keyBy(exit => (exit.id, exit.licensePlate))
      .withSessionWindows(gapDuration)

    val results = boothEntriesById
      .leftOuterJoin(boothExistsById)
      .values
      .map {
        case (boothEntry, Some(boothExit)) =>
          Right(toTotalVehicleTime(boothEntry, boothExit))
        case (boothEntry, None) =>
          // TODO: define constants
          val diagnosticReason = "Missing TollBoothExit to calculate TotalVehicleTime"
          Left(TotalVehicleTimeDiagnostic(boothEntry.id, diagnosticReason))
      }

    results.unzip
  }

  def encodeRecord(input: SCollection[TotalVehicleTime]): SCollection[Record] =
    input.withTimestamp.map { case (r, t) =>
      Record(
        created_at = t,
        license_plate = r.licensePlate.number,
        toll_booth_id = r.tollBoothId.id,
        entry_time = r.entryTime,
        exit_time = r.exitTime,
        duration_seconds = r.duration.getStandardSeconds
      )
    }

  private def toTotalVehicleTime(boothEntry: TollBoothEntry, boothExit: TollBoothExit): TotalVehicleTime = {
    val diff = boothExit.exitTime.getMillis - boothEntry.entryTime.getMillis
    TotalVehicleTime(
      licensePlate = boothEntry.licensePlate,
      tollBoothId = boothEntry.id,
      entryTime = boothEntry.entryTime,
      exitTime = boothExit.exitTime,
      duration = Duration.millis(diff)
    )
  }
}
