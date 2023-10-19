package org.mkuthan.streamprocessing.infrastructure.bigquery.syntax

import scala.reflect.runtime.universe.TypeTag
import scala.reflect.ClassTag
import scala.util.chaining.scalaUtilChainingOps

import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO
import org.apache.beam.sdk.transforms.ParDo

import com.spotify.scio.bigquery.types.BigQueryType
import com.spotify.scio.bigquery.types.BigQueryType.HasAnnotation
import com.spotify.scio.coders.Coder
import com.spotify.scio.values.SCollection

import org.mkuthan.streamprocessing.infrastructure.bigquery.BigQueryDeadLetter
import org.mkuthan.streamprocessing.infrastructure.bigquery.BigQueryDeadLetterEncoderDoFn
import org.mkuthan.streamprocessing.infrastructure.bigquery.BigQueryPartition
import org.mkuthan.streamprocessing.infrastructure.bigquery.BigQueryTable
import org.mkuthan.streamprocessing.infrastructure.bigquery.FileLoadsConfiguration
import org.mkuthan.streamprocessing.infrastructure.bigquery.StorageWriteConfiguration
import org.mkuthan.streamprocessing.infrastructure.common.IoIdentifier
import org.mkuthan.streamprocessing.shared.common.Diagnostic

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

      var deadLetters = self.context
        .withName(s"$id/Empty Dead Letters")
        .empty[BigQueryDeadLetter[T]]()

      val _ = self.betterSaveAsCustomOutput(id.id) { in =>
        val writeResult = in
          .withName("Serialize")
          .map(bigQueryType.toTableRow)
          .internal.apply("Write", io)

        val errors = in.context.wrap(writeResult.getFailedStorageApiInserts)
        deadLetters = errors.applyTransform("Dead Letters", ParDo.of(new BigQueryDeadLetterEncoderDoFn[T]))

        writeResult
      }

      deadLetters
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
    }
  }

  implicit class BigQuerySCollectionDeadLetterOps[T <: AnyRef: Coder](
      private val self: SCollection[BigQueryDeadLetter[T]]
  ) {
    def toDiagnostic(id: IoIdentifier[T]): SCollection[Diagnostic] =
      self.map(deadLetter => Diagnostic(id.id, deadLetter.error))
  }

}
