package org.mkuthan.streamprocessing.shared.common

import org.joda.time.Instant

import org.mkuthan.streamprocessing.shared.common.Diagnostic

trait DiagnosticFixture {

  final val anyDiagnostic: Diagnostic = Diagnostic(
    id = "1",
    reason = "any reason",
    count = 1
  )

  final val anyDiagnosticRecord: Diagnostic.Record = org.mkuthan.streamprocessing.shared.common.Diagnostic.Record(
    created_at = Instant.EPOCH,
    id = anyDiagnostic.id,
    reason = anyDiagnostic.reason,
    count = anyDiagnostic.count
  )
}
