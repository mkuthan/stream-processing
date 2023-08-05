package org.mkuthan.streamprocessing.toll.domain.vehicle

import com.spotify.scio.bigquery.types.BigQueryType
import com.spotify.scio.values.SCollection

import com.twitter.algebird.Semigroup
import org.joda.time.Instant

import org.mkuthan.streamprocessing.shared.scio.pubsub.PubsubMessage
import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothEntry
import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothId
import org.mkuthan.streamprocessing.toll.domain.common.LicensePlate
import org.mkuthan.streamprocessing.toll.domain.registration.VehicleRegistration
import org.mkuthan.streamprocessing.toll.domain.registration.VehicleRegistrationId

final case class VehiclesWithExpiredRegistration(
    licensePlate: LicensePlate,
    tollBoothId: TollBoothId,
    vehicleRegistrationId: VehicleRegistrationId,
    entryTime: Instant
)

object VehiclesWithExpiredRegistration {

  @BigQueryType.toTable
  final case class Raw(
      license_plate: String,
      toll_both_id: String,
      vehicle_registration_id: String,
      entry_time: Instant
  )

  @BigQueryType.toTable
  final case class Diagnostic(
      created_at: Instant,
      reason: String,
      count: Long = 1L
  ) {
    override def toString: String = reason
  }

  final case object DiagnosticSemigroup extends Semigroup[Diagnostic] {
    override def plus(x: Diagnostic, y: Diagnostic): Diagnostic = {
      require(x.toString == y.toString)
      Diagnostic(x.created_at, x.reason, x.count + y.count)
    }
  }

  // TODO: https://github.com/mkuthan/stream-processing/issues/82
  def calculate(
      boothEntries: SCollection[TollBoothEntry],
      vehicleRegistration: SCollection[VehicleRegistration]
  ): (SCollection[VehiclesWithExpiredRegistration], SCollection[Diagnostic]) =
    (boothEntries.context.empty[VehiclesWithExpiredRegistration](), boothEntries.context.empty[Diagnostic]())

  // TODO: implement
  def encode(input: SCollection[VehiclesWithExpiredRegistration]): SCollection[PubsubMessage[Raw]] =
    input.context.empty[PubsubMessage[Raw]]()
}
