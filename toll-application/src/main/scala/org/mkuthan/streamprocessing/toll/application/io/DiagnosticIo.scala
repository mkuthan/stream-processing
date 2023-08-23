package org.mkuthan.streamprocessing.toll.application.io

import org.mkuthan.streamprocessing.infrastructure.common.IoIdentifier
import org.mkuthan.streamprocessing.shared.common.Diagnostic

trait DiagnosticIo {
  val DiagnosticTableIoId: IoIdentifier[Diagnostic.Diagnostic] =
    IoIdentifier[Diagnostic.Diagnostic]("diagnostic-table-id")
}
