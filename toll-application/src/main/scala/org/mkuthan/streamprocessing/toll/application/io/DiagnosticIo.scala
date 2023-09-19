package org.mkuthan.streamprocessing.toll.application.io

import org.mkuthan.streamprocessing.infrastructure.common.IoDiagnostic
import org.mkuthan.streamprocessing.infrastructure.common.IoIdentifier

trait DiagnosticIo {
  val DiagnosticTableIoId: IoIdentifier[IoDiagnostic.Raw] =
    IoIdentifier[IoDiagnostic.Raw]("diagnostic-table-id")
}
