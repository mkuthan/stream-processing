package org.mkuthan.streamprocessing.infrastructure.diagnostic

import scala.reflect.runtime.universe.TypeTag
import scala.util.chaining.scalaUtilChainingOps

import com.spotify.scio.bigquery.types.BigQueryType
import com.spotify.scio.coders.Coder
import com.spotify.scio.values.SCollection

import com.twitter.algebird.Semigroup
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO

import org.mkuthan.streamprocessing.infrastructure.bigquery.BigQueryTable
import org.mkuthan.streamprocessing.infrastructure.common.IoIdentifier

private[diagnostic] class SCollectionOps[K: Coder, V <: BigQueryType.HasAnnotation: Coder: TypeTag: Semigroup](
    private val self: SCollection[(K, V)]
) {

  private val bqType = BigQueryType[V]

  def writeDiagnosticToBigQuery(
      id: IoIdentifier[V],
      table: BigQueryTable[V],
      configuration: DiagnosticConfiguration = DiagnosticConfiguration()
  ): Unit = {
    val io = BigQueryIO
      .writeTableRows()
      .pipe(write => configuration.configure(write))
      .to(table.id)

    val _ = self
      .transform(s"$id/Aggregate") { in =>
        in
          .withFixedWindows(duration = configuration.windowDuration, options = configuration.windowOptions)
          .sumByKey(implicitly[Semigroup[V]])
          .values
      }
      .withName(s"$id/Serialize")
      .map(bqType.toTableRow)
      .saveAsCustomOutput(id.id, io) // ignore errors
  }
}

trait SCollectionSyntax {
  import scala.language.implicitConversions

  implicit def diagnosticSCollectionOps[K: Coder, V <: BigQueryType.HasAnnotation: Coder: TypeTag: Semigroup](
      sc: SCollection[(K, V)]
  ): SCollectionOps[K, V] =
    new SCollectionOps[K, V](sc)
}
