package org.mkuthan.streamprocessing.toll.domain.registration

import com.spotify.scio.values.SCollection
import org.mkuthan.streamprocessing.toll.domain.common.LicensePlate

case class VehicleRegistration(
    id: VehicleRegistrationId,
    licencePlate: LicensePlate,
    expired: Boolean
)

object VehicleRegistration {
  def decode(raw: SCollection[VehicleRegistrationRaw]): (SCollection[VehicleRegistration], SCollection[VehicleRegistrationRaw]) = ???
}
