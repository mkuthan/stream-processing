package org.mkuthan.streamprocessing.toll.application.io

import org.mkuthan.streamprocessing.infrastructure.common.IoIdentifier
import org.mkuthan.streamprocessing.infrastructure.diagnostic.IoDiagnostic

trait DiagnosticIo {
  val DiagnosticTableIoId: IoIdentifier[IoDiagnostic] =
    IoIdentifier[IoDiagnostic]("diagnostic-table-id")
}
