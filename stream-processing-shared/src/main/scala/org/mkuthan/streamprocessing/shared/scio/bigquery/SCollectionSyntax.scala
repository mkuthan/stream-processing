package org.mkuthan.streamprocessing.shared.scio.bigquery

import scala.reflect.runtime.universe.TypeTag
import scala.reflect.ClassTag
import scala.util.chaining.scalaUtilChainingOps

import com.spotify.scio.bigquery.types.BigQueryType
import com.spotify.scio.bigquery.types.BigQueryType.HasAnnotation
import com.spotify.scio.coders.Coder
import com.spotify.scio.values.SCollection

import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write

import org.mkuthan.streamprocessing.shared.scio.common.BigQueryTable
import org.mkuthan.streamprocessing.shared.scio.common.IoIdentifier

private[bigquery] class SCollectionOps[T <: HasAnnotation: Coder: ClassTag: TypeTag](
    private val self: SCollection[T]
) {

  private val bigQueryType = BigQueryType[T]

  def saveToBigQuery(
      ioIdentifier: IoIdentifier,
      table: BigQueryTable[T],
      configuration: FileLoadsConfiguration = FileLoadsConfiguration()
  ): Unit = {
    val io = BigQueryIO
      .writeTableRows()
      .withMethod(Write.Method.FILE_LOADS)
      .withSchema(bigQueryType.schema)
      .pipe(write => configuration.configure(write))
      .to(table.id)

    val _ = self
      .map(bigQueryType.toTableRow)
      .saveAsCustomOutput(ioIdentifier.id, io)
  }
}

trait SCollectionSyntax {
  import scala.language.implicitConversions

  implicit def bigQuerySCollectionOps[T <: HasAnnotation: Coder: ClassTag: TypeTag](
      sc: SCollection[T]
  ): SCollectionOps[T] =
    new SCollectionOps(sc)
}
