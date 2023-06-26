package org.mkuthan.streamprocessing.shared.scio.bigquery

import scala.reflect.runtime.universe.TypeTag
import scala.reflect.ClassTag
import scala.util.chaining.scalaUtilChainingOps

import com.spotify.scio.bigquery.types.BigQueryType
import com.spotify.scio.bigquery.types.BigQueryType.HasAnnotation
import com.spotify.scio.coders.Coder
import com.spotify.scio.values.SCollection

import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO
import org.apache.beam.sdk.transforms.ParDo

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
  ): SCollection[BigQueryDeadLetter[T]] = {
    val io = BigQueryIO
      .writeTableRows()
      .withSchema(bigQueryType.schema)
      .pipe(write => configuration.configure(table.id, write))

    self.transform(ioIdentifier.id) { in =>
      val results = in
        .map(bigQueryType.toTableRow)
        .internal.apply(io)

      val failedRows = self.context.wrap(results.getFailedInserts)
        .map(failedRow => (failedRow, "Unknown error"))
      failedRows.applyTransform(ParDo.of(new BigQueryDeadLetterEncoderDoFn[T]()))
    }
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

    self.transform(ioIdentifier.id) { in =>
      val results = in
        .map(bigQueryType.toTableRow)
        .internal.apply(io)

      val failedRows = self.context.wrap(results.getFailedStorageApiInserts)
        .map(failedRow => (failedRow.getRow, failedRow.getErrorMessage))

      failedRows.applyTransform(ParDo.of(new BigQueryDeadLetterEncoderDoFn[T]()))
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
