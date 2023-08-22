package org.mkuthan.streamprocessing.toll.application.io

import org.mkuthan.streamprocessing.infrastructure.common.IoDiagnostic
import org.mkuthan.streamprocessing.infrastructure.common.IoIdentifier

trait IoDiagnosticIo {
  val IoDiagnosticTableIoId: IoIdentifier[IoDiagnostic.Diagnostic] =
    IoIdentifier[IoDiagnostic.Diagnostic]("io-diagnostic-table-id")
}
