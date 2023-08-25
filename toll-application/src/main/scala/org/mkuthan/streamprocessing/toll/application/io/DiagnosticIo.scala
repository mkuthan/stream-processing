package org.mkuthan.streamprocessing.toll.application.io

import org.mkuthan.streamprocessing.infrastructure.common.IoIdentifier
import org.mkuthan.streamprocessing.shared.common.Diagnostic

trait DiagnosticIo {
  val DiagnosticTableIoId: IoIdentifier[Diagnostic.Raw] =
    IoIdentifier[Diagnostic.Raw]("diagnostic-table-id")
}
