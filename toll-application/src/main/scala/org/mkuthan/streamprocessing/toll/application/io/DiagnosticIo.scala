package org.mkuthan.streamprocessing.toll.application.io

import org.mkuthan.streamprocessing.infrastructure.common.IoIdentifier
import org.mkuthan.streamprocessing.infrastructure.diagnostic.IoDiagnostic

trait DiagnosticIo {
  val DiagnosticTableIoId: IoIdentifier[IoDiagnostic.Raw] =
    IoIdentifier[IoDiagnostic.Raw]("diagnostic-table-id")
}
