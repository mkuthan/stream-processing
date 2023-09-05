package org.mkuthan.streamprocessing.infrastructure.bigquery

import scala.reflect.runtime.universe.TypeTag
import scala.reflect.ClassTag
import scala.util.chaining.scalaUtilChainingOps

import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO
import org.apache.beam.sdk.transforms.ParDo

import com.spotify.scio.bigquery.types.BigQueryType
import com.spotify.scio.bigquery.types.BigQueryType.HasAnnotation
import com.spotify.scio.coders.Coder
import com.spotify.scio.values.SCollection

import org.mkuthan.streamprocessing.infrastructure.common.IoIdentifier
import org.mkuthan.streamprocessing.infrastructure.diagnostic.IoDiagnostic

trait BigQuerySCollectionSyntax {

  import scala.language.implicitConversions

  implicit def bigQuerySCollectionOps[T <: HasAnnotation: Coder: ClassTag: TypeTag](
      sc: SCollection[T]
  ): SCollectionOps[T] =
    new SCollectionOps(sc)

  implicit def bigQuerySCollectionDeadLetterOps[T <: AnyRef: Coder](
      sc: SCollection[BigQueryDeadLetter[T]]
  ): SCollectionDeadLetterOps[T] =
    new SCollectionDeadLetterOps(sc)
}

private[bigquery] class SCollectionOps[T <: HasAnnotation: Coder: ClassTag: TypeTag](
    private val self: SCollection[T]
) {

  private val bigQueryType = BigQueryType[T]

  def writeUnboundedToBigQuery(
      id: IoIdentifier[T],
      table: BigQueryTable[T],
      configuration: StorageWriteConfiguration = StorageWriteConfiguration()
  ): SCollection[BigQueryDeadLetter[T]] = {
    val io = BigQueryIO
      .writeTableRows()
      .pipe(write => configuration.configure(write))
      .to(table.id)

    self.transform(id.id) { in =>
      val results = in
        .withName("Serialize")
        .map(bigQueryType.toTableRow)
        .internal.apply("Write to BQ", io)

      val errors = self.context.wrap(results.getFailedStorageApiInserts)
        .withName("Extract errors")
        .map(failedRow => (failedRow.getRow, failedRow.getErrorMessage))

      errors.applyTransform("Create dead letters", ParDo.of(new BigQueryDeadLetterEncoderDoFn[T](id)))
    }
  }

  def writeBoundedToBigQuery(
      id: IoIdentifier[T],
      partition: BigQueryPartition[T],
      configuration: FileLoadsConfiguration = FileLoadsConfiguration()
  ): Unit = {
    val io = BigQueryIO
      .writeTableRows()
      .pipe(write => configuration.configure(write))
      .to(partition.id)

    val _ = self
      .withName(s"$id/Serialize")
      .map(bigQueryType.toTableRow)
      .saveAsCustomOutput(id.id, io)
  }
}

private[bigquery] class SCollectionDeadLetterOps[T <: AnyRef: Coder](
    private val self: SCollection[BigQueryDeadLetter[T]]
) {
  def toDiagnostic(): SCollection[IoDiagnostic] =
    self.map(deadLetter => IoDiagnostic(deadLetter.id.id, deadLetter.error))
}
