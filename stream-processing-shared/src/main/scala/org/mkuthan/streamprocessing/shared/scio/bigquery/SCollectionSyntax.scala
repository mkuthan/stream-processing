package org.mkuthan.streamprocessing.shared.scio.bigquery

import scala.reflect.runtime.universe.TypeTag
import scala.reflect.ClassTag
import scala.util.chaining.scalaUtilChainingOps

import com.spotify.scio.bigquery.types.BigQueryType
import com.spotify.scio.bigquery.types.BigQueryType.HasAnnotation
import com.spotify.scio.coders.Coder
import com.spotify.scio.values.SCollection

import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO

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
      .withSchema(bigQueryType.schema)
      .pipe(write => configuration.configure(table.id, write))

    val _ = self
      .map(bigQueryType.toTableRow)
      .saveAsCustomOutput(ioIdentifier.id, io)
  }

  def saveToBigQueryStorage(
      ioIdentifier: IoIdentifier,
      table: BigQueryTable[T],
      configuration: StorageWriteConfiguration = StorageWriteConfiguration()
  ): SCollection[BigQueryDeadLetter[T]] = {
    val io = BigQueryIO
      .writeTableRows()
      .withSchema(bigQueryType.schema)
      .pipe(write => configuration.configure(table.id, write))

    val results = self
      .map(bigQueryType.toTableRow)
      .internal.apply(ioIdentifier.id, io) // TODO: there is no way to use customOutput here

    val failedRows = self.context.wrap(results.getFailedStorageApiInserts)
    failedRows.map { failedRow =>
      // TODO: how to serialize the original row?
      // val row = bigQueryType.fromTableRow(failedRow.getRow)
      val error = failedRow.getErrorMessage

      BigQueryDeadLetter(error)
    }
  }
}

trait SCollectionSyntax {
  import scala.language.implicitConversions

  implicit def bigQuerySCollectionOps[T <: HasAnnotation: Coder: ClassTag: TypeTag](
      sc: SCollection[T]
  ): SCollectionOps[T] =
    new SCollectionOps(sc)
}
