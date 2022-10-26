package org.mkuthan.streamprocessing.toll.domain.registration

import com.spotify.scio.bigquery.types.BigQueryType
import com.spotify.scio.values.SCollection

import org.mkuthan.streamprocessing.toll.domain.common.LicensePlate

final case class VehicleRegistration(
    id: VehicleRegistrationId,
    licencePlate: LicensePlate,
    expired: Boolean
)

object VehicleRegistration {

  // implicit val CoderCache: Coder[VehicleRegistration] = Coder.gen

  @BigQueryType.toTable
  final case class Raw(
      id: String,
      licence_plate: String,
      expired: Int
  )

  def decode(raw: SCollection[Raw]): (SCollection[VehicleRegistration], SCollection[Raw]) =
    (raw.context.empty[VehicleRegistration](), raw.context.empty[Raw]())
}
