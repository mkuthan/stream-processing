package org.mkuthan.streamprocessing.toll.domain.registration

final case class VehicleRegistrationId(id: String) {
  require(!id.isEmpty, "Vehicle registration id is empty")
}
