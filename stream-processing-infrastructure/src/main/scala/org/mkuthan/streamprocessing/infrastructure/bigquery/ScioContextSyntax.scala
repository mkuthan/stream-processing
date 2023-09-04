package org.mkuthan.streamprocessing.infrastructure.bigquery

import scala.reflect.runtime.universe.TypeTag
import scala.reflect.ClassTag
import scala.util.chaining.scalaUtilChainingOps

import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO

import com.spotify.scio.bigquery.types.BigQueryType
import com.spotify.scio.bigquery.types.BigQueryType.HasAnnotation
import com.spotify.scio.coders.Coder
import com.spotify.scio.values.SCollection
import com.spotify.scio.ScioContext

import org.mkuthan.streamprocessing.infrastructure.common.IoIdentifier

private[bigquery] class ScioContextOps(private val self: ScioContext) extends AnyVal {

  def queryFromBigQuery[T <: HasAnnotation: Coder: ClassTag: TypeTag](
      id: IoIdentifier[T],
      query: BigQueryQuery[T],
      configuration: ExportConfiguration = ExportConfiguration()
  ): SCollection[T] = {
    val io = BigQueryIO
      .readTableRows()
      .pipe(read => configuration.configure(read))
      .fromQuery(query.query)

    val bigQueryType = BigQueryType[T]

    self
      .customInput(id.id, io)
      .withName(s"$id/Deserialize")
      .map(bigQueryType.fromTableRow)
  }

  def readFromBigQuery[T <: HasAnnotation: Coder: ClassTag: TypeTag](
      id: IoIdentifier[T],
      table: BigQueryTable[T],
      configuration: StorageReadConfiguration = StorageReadConfiguration()
  ): SCollection[T] = {
    val io = BigQueryIO
      .readTableRows()
      .pipe(read => configuration.configure(read))
      .from(table.id)

    val bigQueryType = BigQueryType[T]

    self
      .customInput(id.id, io)
      .withName(s"$id/Deserialize")
      .map(bigQueryType.fromTableRow)
  }
}

trait ScioContextSyntax {
  import scala.language.implicitConversions

  implicit def bigQueryScioContextOps(sc: ScioContext): ScioContextOps = new ScioContextOps(sc)
}
