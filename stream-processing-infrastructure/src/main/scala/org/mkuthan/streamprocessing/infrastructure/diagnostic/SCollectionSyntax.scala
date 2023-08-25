package org.mkuthan.streamprocessing.infrastructure.diagnostic

import scala.reflect.runtime.universe.TypeTag
import scala.util.chaining.scalaUtilChainingOps

import com.spotify.scio.bigquery.types.BigQueryType
import com.spotify.scio.coders.Coder
import com.spotify.scio.values.SCollection

import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO

import org.mkuthan.streamprocessing.infrastructure.bigquery.BigQueryTable
import org.mkuthan.streamprocessing.infrastructure.common.IoIdentifier
import org.mkuthan.streamprocessing.shared.common.SumByKey

private[diagnostic] class SCollectionOps[T: Coder: TypeTag: SumByKey](
    private val self: SCollection[T]
) {

  private val bqType = BigQueryType[T]

  def writeDiagnosticToBigQuery(
      id: IoIdentifier[T],
      table: BigQueryTable[T],
      configuration: DiagnosticConfiguration = DiagnosticConfiguration()
  ): Unit = {
    val io = BigQueryIO
      .writeTableRows()
      .pipe(write => configuration.configure(write))
      .to(table.id)

    val _ = self
      .withName(s"$id/Key")
      .keyBy(SumByKey[T].key)
      .transform(s"$id/Aggregate") { in =>
        in
          .withFixedWindows(duration = configuration.windowDuration, options = configuration.windowOptions)
          .sumByKey(SumByKey[T].semigroup)
          .values
      }
      .withName(s"$id/Serialize")
      .map(bqType.toTableRow)
      .saveAsCustomOutput(id.id, io) // ignore errors
  }
}

trait SCollectionSyntax {

  import scala.language.implicitConversions

  implicit def diagnosticSCollectionOps[T: Coder: TypeTag: SumByKey](sc: SCollection[T]): SCollectionOps[T] =
    new SCollectionOps[T](sc)
}
