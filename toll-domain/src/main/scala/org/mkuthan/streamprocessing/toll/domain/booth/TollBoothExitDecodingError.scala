package org.mkuthan.streamprocessing.toll.domain.booth

final case class TollBoothExitDecodingError(data: TollBoothExit.Raw, error: String)
