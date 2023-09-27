package org.mkuthan.streamprocessing.infrastructure.common

import org.joda.time.Instant

trait IoDiagnosticFixture {

  final val anyIoDiagnostic: IoDiagnostic = IoDiagnostic(
    id = "1",
    reason = "any reason",
    count = 1
  )

  final val anyIoDiagnosticRecord: IoDiagnostic.Record = IoDiagnostic.Record(
    created_at = Instant.EPOCH,
    id = anyIoDiagnostic.id,
    reason = anyIoDiagnostic.reason,
    count = anyIoDiagnostic.count
  )
}
