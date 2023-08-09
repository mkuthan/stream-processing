package org.mkuthan.streamprocessing.toll.domain.vehicle

import com.spotify.scio.bigquery.types.BigQueryType
import com.spotify.scio.values.SCollection
import com.spotify.scio.values.SideOutput

import com.twitter.algebird.Semigroup
import org.joda.time.Duration
import org.joda.time.Instant

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
  case class Raw(
      record_timestamp: Instant,
      license_plate: String,
      toll_booth_id: String,
      entry_time: Instant,
      exit_time: Instant,
      duration_seconds: Long
  )

  type DiagnosticKey = String

  @BigQueryType.toTable
  case class Diagnostic(
      created_at: Instant,
      toll_both_id: String,
      reason: String,
      count: Long = 1L
  ) {
    lazy val key: String = toll_both_id + reason
  }

  implicit case object DiagnosticSemigroup extends Semigroup[Diagnostic] {
    override def plus(x: Diagnostic, y: Diagnostic): Diagnostic = {
      require(x.key == y.key)
      Diagnostic(x.created_at, x.toll_both_id, x.reason, x.count + y.count)
    }
  }

  def calculateInSessionWindow(
      boothEntries: SCollection[TollBoothEntry],
      boothExits: SCollection[TollBoothExit],
      gapDuration: Duration
  ): (SCollection[TotalVehicleTime], SCollection[(DiagnosticKey, Diagnostic)]) = {
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
          val diagnosticReason = "Missing TollBoothExit to calculate TotalVehicleTime"
          ctx.output(diagnostic, toDiagnostic(boothEntry, diagnosticReason))
          None
      }
    (results, sideOutputs(diagnostic).keyBy(_.key))
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

  private def toDiagnostic(boothEntry: TollBoothEntry, reason: String): Diagnostic =
    Diagnostic(
      created_at = boothEntry.entryTime,
      toll_both_id = boothEntry.id.id,
      reason = reason
    )
}
