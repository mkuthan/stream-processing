package org.mkuthan.streamprocessing.shared.scio.bigquery

import scala.language.implicitConversions
import scala.reflect.runtime.universe.TypeTag
import scala.reflect.ClassTag
import scala.util.chaining.scalaUtilChainingOps

import com.spotify.scio.bigquery.types.BigQueryType
import com.spotify.scio.bigquery.types.BigQueryType.HasAnnotation
import com.spotify.scio.coders.Coder
import com.spotify.scio.values.SCollection
import com.spotify.scio.ScioContext

import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.TypedRead

import org.mkuthan.streamprocessing.shared.scio.common.BigQueryTable
import org.mkuthan.streamprocessing.shared.scio.common.IoIdentifier

private[bigquery] class ScioContextOps(private val self: ScioContext) extends AnyVal {
  def loadFromBigQuery[T <: HasAnnotation: Coder: ClassTag: TypeTag](
      ioIdentifier: IoIdentifier,
      table: BigQueryTable[T],
      configuration: ExportConfiguration = ExportConfiguration()
  ): SCollection[T] = {
    val io = org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO
      .readTableRows()
      .withMethod(TypedRead.Method.EXPORT)
      .pipe(read => configuration.configure(read, table.id))

    val bigQueryType = BigQueryType[T]

    self
      .customInput(ioIdentifier.id, io)
      .map(bigQueryType.fromTableRow)
  }

  def loadFromBigQueryStorage[T <: HasAnnotation: Coder: ClassTag: TypeTag](
      ioIdentifier: IoIdentifier,
      table: BigQueryTable[T],
      configuration: StorageReadConfiguration = StorageReadConfiguration()
  ): SCollection[T] = {
    val io = org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO
      .readTableRows()
      .withMethod(TypedRead.Method.DIRECT_READ)
      .pipe(read => configuration.configure(read))
      .from(table.id)

    val bigQueryType = BigQueryType[T]

    self
      .customInput(ioIdentifier.id, io)
      .map(bigQueryType.fromTableRow)
  }
}

trait ScioContextSyntax {
  implicit def bigQueryScioContextOps(sc: ScioContext): ScioContextOps = new ScioContextOps(sc)
}
