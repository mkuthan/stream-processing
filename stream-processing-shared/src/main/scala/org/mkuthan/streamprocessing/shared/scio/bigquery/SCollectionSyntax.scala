package org.mkuthan.streamprocessing.shared.scio.bigquery

import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag
import scala.util.chaining.scalaUtilChainingOps

import com.spotify.scio.bigquery.types.BigQueryType
import com.spotify.scio.bigquery.types.BigQueryType.HasAnnotation
import com.spotify.scio.coders.Coder
import com.spotify.scio.values.SCollection
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write
import org.apache.beam.sdk.transforms.ParDo
import org.apache.beam.sdk.values.PCollection.IsBounded
import org.mkuthan.streamprocessing.shared.scio.common.BigQueryTable
import org.mkuthan.streamprocessing.shared.scio.common.IoIdentifier

private[bigquery] class SCollectionOps[T <: HasAnnotation: Coder: ClassTag: TypeTag](
    private val self: SCollection[T]
) {

  private val bigQueryType = BigQueryType[T]

  def writeToBigQuery(
      id: IoIdentifier,
      table: BigQueryTable[T],
      configuration: StorageWriteConfiguration = StorageWriteConfiguration()
  ): SCollection[BigQueryDeadLetter[T]] = {
    val io = BigQueryIO
      .writeTableRows()
      .pipe(write => configuration.configure(write))
      .pipe(write => configureWriteMethod(write))
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

  private def configureWriteMethod[U](write: Write[U]): Write[U] = {
    val method = if (self.internal.isBounded == IsBounded.BOUNDED) {
      Write.Method.STORAGE_WRITE_API
    } else {
      Write.Method.STORAGE_API_AT_LEAST_ONCE
    }
    write.withMethod(method)
  }
}

trait SCollectionSyntax {
  import scala.language.implicitConversions

  implicit def bigQuerySCollectionOps[T <: HasAnnotation: Coder: ClassTag: TypeTag](
      sc: SCollection[T]
  ): SCollectionOps[T] =
    new SCollectionOps(sc)
}
