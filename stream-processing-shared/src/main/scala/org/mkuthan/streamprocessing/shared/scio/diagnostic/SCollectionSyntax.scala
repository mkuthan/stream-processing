package org.mkuthan.streamprocessing.shared.scio.diagnostic

import scala.reflect.runtime.universe.TypeTag
import scala.util.chaining.scalaUtilChainingOps

import com.spotify.scio.bigquery.types.BigQueryType
import com.spotify.scio.coders.Coder
import com.spotify.scio.values.SCollection

import com.twitter.algebird.Semigroup
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO

import org.mkuthan.streamprocessing.shared.scio.common.BigQueryTable
import org.mkuthan.streamprocessing.shared.scio.common.IoIdentifier

private[diagnostic] class SCollectionOps[T <: BigQueryType.HasAnnotation: Coder: TypeTag](
    private val self: SCollection[T]
) {

  private val bqType = BigQueryType[T]

  def writeDiagnosticToBigQuery(
      id: IoIdentifier[T],
      table: BigQueryTable[T],
      sg: Semigroup[T], // TODO: define as implicit parameter
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
          .keyBy(_.toString) // TODO: how to make it more generic?
          .sumByKey(sg)
          .values
      }
      .withName(s"$id/Serialize")
      .map(bqType.toTableRow)
      .saveAsCustomOutput(id.id, io) // ignore errors
  }
}

trait SCollectionSyntax {
  import scala.language.implicitConversions

  implicit def diagnosticSCollectionOps[T <: BigQueryType.HasAnnotation: Coder: TypeTag](
      sc: SCollection[T]
  ): SCollectionOps[T] =
    new SCollectionOps[T](sc)
}
