package org.mkuthan.streamprocessing.toll.domain.common

final case class LicensePlate(number: String) {
  require(!number.isEmpty, "Licence plate number is empty")
}
