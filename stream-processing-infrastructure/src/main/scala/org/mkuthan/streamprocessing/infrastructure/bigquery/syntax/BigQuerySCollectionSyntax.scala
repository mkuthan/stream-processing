package org.mkuthan.streamprocessing.infrastructure.bigquery.syntax

import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag
import scala.util.chaining.scalaUtilChainingOps

import com.spotify.scio.bigquery.types.BigQueryType
import com.spotify.scio.bigquery.types.BigQueryType.HasAnnotation
import com.spotify.scio.coders.Coder
import com.spotify.scio.values.SCollection
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO
import org.apache.beam.sdk.transforms.ParDo
import org.mkuthan.streamprocessing.infrastructure.bigquery.BigQueryDeadLetter
import org.mkuthan.streamprocessing.infrastructure.bigquery.BigQueryDeadLetterEncoderDoFn
import org.mkuthan.streamprocessing.infrastructure.bigquery.BigQueryPartition
import org.mkuthan.streamprocessing.infrastructure.bigquery.BigQueryTable
import org.mkuthan.streamprocessing.infrastructure.bigquery.FileLoadsConfiguration
import org.mkuthan.streamprocessing.infrastructure.bigquery.StorageWriteConfiguration
import org.mkuthan.streamprocessing.infrastructure.common.IoIdentifier
import org.mkuthan.streamprocessing.infrastructure.diagnostic.IoDiagnostic

private[syntax] trait BigQuerySCollectionSyntax {

  implicit class BigQuerySCollectionOps[T <: HasAnnotation: Coder: ClassTag: TypeTag](
      private val self: SCollection[T]
  ) {

    import com.spotify.scio.values.BetterSCollection._

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

      var deadLetters = self.context.empty[BigQueryDeadLetter[T]]

      val _ = self.betterSaveAsCustomOutput(id.id) { in =>
        val writeResult = in
          .withName("Serialize")
          .map(bigQueryType.toTableRow)
          .internal.apply("Write", io)

        val errors = in.context.wrap(writeResult.getFailedStorageApiInserts)
          .withName("Extract errors")
          .map(failedRow => (failedRow.getRow, failedRow.getErrorMessage))

        deadLetters = errors.applyTransform("Create dead letters", ParDo.of(new BigQueryDeadLetterEncoderDoFn[T](id)))

        writeResult
      }

      deadLetters

//      self.transform(id.id) { in =>
//        val results = in
//          .withName("Serialize")
//          .map(bigQueryType.toTableRow)
//          .internal.apply("Write to BQ", io)
//
//        val errors = self.context.wrap(results.getFailedStorageApiInserts)
//          .withName("Extract errors")
//          .map(failedRow => (failedRow.getRow, failedRow.getErrorMessage))
//
//        errors.applyTransform("Create dead letters", ParDo.of(new BigQueryDeadLetterEncoderDoFn[T](id)))
//      }
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

      val _ = self.betterSaveAsCustomOutput(id.id) { in =>
        in
          .withName("Serialize")
          .map(bigQueryType.toTableRow)
          .internal.apply("Write", io)
      }

//      val _ = self
//        .withName(s"$id/Serialize")
//        .map(bigQueryType.toTableRow)
//        .saveAsCustomOutput(id.id, io)
    }
  }

  implicit class BigQuerySCollectionDeadLetterOps[T <: AnyRef: Coder](
      private val self: SCollection[BigQueryDeadLetter[T]]
  ) {
    def toDiagnostic(): SCollection[IoDiagnostic] =
      self.map(deadLetter => IoDiagnostic(deadLetter.id.id, deadLetter.error))
  }

}
