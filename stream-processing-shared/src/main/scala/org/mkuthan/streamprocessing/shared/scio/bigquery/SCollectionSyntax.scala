package org.mkuthan.streamprocessing.shared.scio.bigquery

import scala.reflect.runtime.universe.TypeTag
import scala.reflect.ClassTag

import com.spotify.scio.bigquery.types.BigQueryType
import com.spotify.scio.bigquery.types.BigQueryType.HasAnnotation
import com.spotify.scio.coders.Coder
import com.spotify.scio.values.SCollection

import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO

import org.mkuthan.streamprocessing.shared.configuration.BigQueryTable

private[bigquery] class SCollectionOps[T <: HasAnnotation: Coder: ClassTag: TypeTag](
    private val self: SCollection[T]
) {

  private val bigQueryType = BigQueryType[T]

  def saveToBigQuery(table: BigQueryTable[T]): Unit = {
    val io = BigQueryIO
      .writeTableRows()
      .withSchema(bigQueryType.schema)
      .to(table.id)

    val _ = self
      .map(bigQueryType.toTableRow)
      .saveAsCustomOutput(table.id, io)
  }
}

trait SCollectionSyntax {
  import scala.language.implicitConversions

  implicit def bigQuerySCollectionOps[T <: HasAnnotation: Coder: ClassTag: TypeTag](
      sc: SCollection[T]
  ): SCollectionOps[T] =
    new SCollectionOps(sc)
}
