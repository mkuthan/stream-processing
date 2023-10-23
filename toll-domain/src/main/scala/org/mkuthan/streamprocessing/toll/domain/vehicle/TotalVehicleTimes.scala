package org.mkuthan.streamprocessing.toll.domain.vehicle

import com.spotify.scio.bigquery.types.BigQueryType
import com.spotify.scio.values.SCollection
import com.spotify.scio.values.WindowOptions

import org.joda.time.Duration
import org.joda.time.Instant

import org.mkuthan.streamprocessing.shared.scio.syntax._
import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothDiagnostic
import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothEntry
import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothExit
import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothId
import org.mkuthan.streamprocessing.toll.domain.common.LicensePlate

final case class TotalVehicleTimes(
    licensePlate: LicensePlate,
    tollBoothId: TollBoothId,
    entryTime: Instant,
    exitTime: Instant,
    duration: Duration
)

object TotalVehicleTimes {

  @BigQueryType.toTable
  final case class Record(
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
      gapDuration: Duration,
      windowOptions: WindowOptions
  ): (SCollection[TotalVehicleTimes], SCollection[TollBoothDiagnostic]) = {
    val results = boothEntries.transform { in =>
      val boothEntriesById = in
        .keyBy(entry => (entry.id, entry.licensePlate))
        .withSessionWindows(gapDuration = gapDuration, options = windowOptions)

      val boothExistsById = boothExits
        .keyBy(exit => (exit.id, exit.licensePlate))
        .withSessionWindows(gapDuration = gapDuration, options = windowOptions)

      boothEntriesById
        .leftOuterJoin(boothExistsById)
        .values
        .map {
          case (boothEntry, Some(boothExit)) =>
            Right(toTotalVehicleTimes(boothEntry, boothExit))
          case (boothEntry, None) =>
            Left(TollBoothDiagnostic(boothEntry.id, TollBoothDiagnostic.MissingTollBoothExit))
        }
    }

    results.unzip
  }

  def encodeRecord(input: SCollection[TotalVehicleTimes]): SCollection[Record] =
    input.mapWithTimestamp { case (r, t) =>
      Record(
        created_at = t,
        license_plate = r.licensePlate.number,
        toll_booth_id = r.tollBoothId.id,
        entry_time = r.entryTime,
        exit_time = r.exitTime,
        duration_seconds = r.duration.getStandardSeconds
      )
    }

  private def toTotalVehicleTimes(boothEntry: TollBoothEntry, boothExit: TollBoothExit): TotalVehicleTimes = {
    val diff = boothExit.exitTime.getMillis - boothEntry.entryTime.getMillis
    TotalVehicleTimes(
      licensePlate = boothEntry.licensePlate,
      tollBoothId = boothEntry.id,
      entryTime = boothEntry.entryTime,
      exitTime = boothExit.exitTime,
      duration = Duration.millis(diff)
    )
  }
}
