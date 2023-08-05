package org.mkuthan.streamprocessing.shared.scio.diagnostic

import scala.util.chaining.scalaUtilChainingOps

import com.spotify.scio.values.SCollection

import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO

import org.mkuthan.streamprocessing.shared.scio.common.BigQueryTable
import org.mkuthan.streamprocessing.shared.scio.common.IoIdentifier

private[diagnostic] class SCollectionOps(private val self: SCollection[Diagnostic]) {

  def writeDiagnosticToBigQuery(
      id: IoIdentifier[Diagnostic],
      table: BigQueryTable[Diagnostic],
      configuration: DiagnosticConfiguration = DiagnosticConfiguration()
  ): Unit = {
    val io = BigQueryIO
      .writeTableRows()
      .pipe(write => configuration.configure(write))
      .to(table.id)

    val _ = self
      .transform(s"$id/Aggregate") { in =>
        Diagnostic.aggregateInFixedWindow(in, configuration.windowDuration, configuration.windowOptions)
      }
      .transform(s"$id/Serialize") { in =>
        Diagnostic.serialize(in)
      }
      .saveAsCustomOutput(id.id, io)
  }
}

trait SCollectionSyntax {
  import scala.language.implicitConversions

  implicit def diagnosticSCollectionOps(sc: SCollection[Diagnostic]): SCollectionOps =
    new SCollectionOps(sc)
}
