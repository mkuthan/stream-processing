package org.mkuthan.streamprocessing.toll.domain.booth

import org.joda.time.Instant

import org.mkuthan.streamprocessing.toll.domain.booth.TollBoothId

trait TollBoothDiagnosticFixture {

  final val anyTollBoothDiagnostic: TollBoothDiagnostic =
    TollBoothDiagnostic(
      tollBoothId = TollBoothId("1"),
      reason = "any reason",
      count = 1
    )

  final val anyTollBoothDiagnosticRecord: TollBoothDiagnostic.Record =
    TollBoothDiagnostic.Record(
      created_at = Instant.EPOCH,
      toll_booth_id = anyTollBoothDiagnostic.tollBoothId.id,
      reason = anyTollBoothDiagnostic.reason,
      count = anyTollBoothDiagnostic.count
    )
}
