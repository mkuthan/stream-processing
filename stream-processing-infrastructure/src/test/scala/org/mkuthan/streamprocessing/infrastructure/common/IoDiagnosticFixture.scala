package org.mkuthan.streamprocessing.infrastructure.common

import org.joda.time.Instant

trait IoDiagnosticFixture {

  final val anyDiagnostic: IoDiagnostic = IoDiagnostic(
    id = "1",
    reason = "any reason",
    count = 1
  )

  final val anyDiagnosticRecord: IoDiagnostic.Record = IoDiagnostic.Record(
    created_at = Instant.EPOCH,
    id = anyDiagnostic.id,
    reason = anyDiagnostic.reason,
    count = anyDiagnostic.count
  )
}
