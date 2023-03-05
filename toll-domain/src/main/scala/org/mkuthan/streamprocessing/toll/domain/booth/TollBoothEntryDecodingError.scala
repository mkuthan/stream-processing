package org.mkuthan.streamprocessing.toll.domain.booth

final case class TollBoothEntryDecodingError(data: TollBoothEntry.Raw, error: String)
