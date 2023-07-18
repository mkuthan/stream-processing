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

import org.mkuthan.streamprocessing.shared.scio.common.BigQueryPartition
import org.mkuthan.streamprocessing.shared.scio.common.BigQueryTable
import org.mkuthan.streamprocessing.shared.scio.common.IoIdentifier

private[bigquery] class SCollectionOps[T <: HasAnnotation: Coder: ClassTag: TypeTag](
    private val self: SCollection[T]
) {

  private val bigQueryType = BigQueryType[T]

  def writeUnboundedToBigQuery(
      id: IoIdentifier,
      table: BigQueryTable[T],
      configuration: StorageWriteConfiguration = StorageWriteConfiguration()
  ): SCollection[BigQueryDeadLetter[T]] = {
    val io = BigQueryIO
      .writeTableRows()
      .pipe(write => configuration.configure(write))
      .withSchema(bigQueryType.schema)
      .to(table.id)

    self.transform(id.id) { in =>
      val results = in
        .withName("Serialize")
        .map(bigQueryType.toTableRow)
        .internal.apply("Write to BQ", io)

      val errors = self.context.wrap(results.getFailedStorageApiInserts)
        .withName("Extract errors")
        .map(failedRow => (failedRow.getRow, failedRow.getErrorMessage))

      errors.applyTransform("Create dead letters", ParDo.of(new BigQueryDeadLetterEncoderDoFn[T]()))
    }
  }

  def writeBoundedToBigQuery(
      id: IoIdentifier,
      partition: BigQueryPartition[T],
      configuration: FileLoadsConfiguration = FileLoadsConfiguration()
  ): Unit = {
    val io = BigQueryIO
      .writeTableRows()
      .pipe(write => configuration.configure(write))
      .withSchema(bigQueryType.schema)
      .to(partition.id)

    self
      .withName(s"$id/Serialize")
      .map(bigQueryType.toTableRow)
      .saveAsCustomOutput(id.id, io)
  }

}

trait SCollectionSyntax {
  import scala.language.implicitConversions

  implicit def bigQuerySCollectionOps[T <: HasAnnotation: Coder: ClassTag: TypeTag](
      sc: SCollection[T]
  ): SCollectionOps[T] =
    new SCollectionOps(sc)
}
