package org.mkuthan.streamprocessing.toll.domain.booth

final case class TollBoothId(id: String) {
  require(!id.isEmpty, "Toll booth id is empty")
}
