package org.mkuthan.streamprocessing.infrastructure.diagnostic

import scala.reflect.runtime.universe.TypeTag
import scala.reflect.ClassTag
import scala.util.chaining.scalaUtilChainingOps

import com.spotify.scio.bigquery.types.BigQueryType
import com.spotify.scio.bigquery.types.BigQueryType.HasAnnotation
import com.spotify.scio.coders.Coder
import com.spotify.scio.values.SCollection

import com.google.api.services.bigquery.model.TableRow
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO
import org.joda.time.Instant

import org.mkuthan.streamprocessing.infrastructure.bigquery.BigQueryPartition
import org.mkuthan.streamprocessing.infrastructure.bigquery.BigQueryTable
import org.mkuthan.streamprocessing.infrastructure.bigquery.FileLoadsConfiguration
import org.mkuthan.streamprocessing.infrastructure.bigquery.WriteDisposition
import org.mkuthan.streamprocessing.infrastructure.common.IoIdentifier
import org.mkuthan.streamprocessing.shared.common.SumByKey

private[diagnostic] class SCollectionOps[D: Coder: SumByKey](
    private val self: SCollection[D]
) {

  private val bqConfiguration = FileLoadsConfiguration()

  def writeUnboundedDiagnosticToBigQuery[R <: HasAnnotation: Coder: ClassTag: TypeTag](
      id: IoIdentifier[D],
      table: BigQueryTable[D],
      toBigQueryTypeFn: (D, Instant) => R,
      configuration: DiagnosticConfiguration = DiagnosticConfiguration()
  ): Unit = {
    val io = BigQueryIO
      .writeTableRows()
      .withTriggeringFrequency(configuration.windowDuration)
      .pipe(write => bqConfiguration.withWriteDisposition(WriteDisposition.Append).configure(write))
      .to(table.id)

    writeDiagnosticToBigQuery(id, toBigQueryTypeFn, configuration, io)
  }

  def writeBoundedDiagnosticToBigQuery[R <: HasAnnotation: Coder: ClassTag: TypeTag](
      id: IoIdentifier[D],
      partition: BigQueryPartition[D],
      toBigQueryTypeFn: (D, Instant) => R,
      configuration: DiagnosticConfiguration = DiagnosticConfiguration()
  ): Unit = {
    val io = BigQueryIO
      .writeTableRows()
      .pipe(write => bqConfiguration.withWriteDisposition(WriteDisposition.Truncate).configure(write))
      .to(partition.id)

    writeDiagnosticToBigQuery(id, toBigQueryTypeFn, configuration, io)
  }

  private def writeDiagnosticToBigQuery[R <: HasAnnotation: Coder: ClassTag: TypeTag](
      id: IoIdentifier[D],
      toBigQueryTypeFn: (D, Instant) => R,
      configuration: DiagnosticConfiguration,
      io: BigQueryIO.Write[TableRow]
  ): Unit = {
    val bqType = BigQueryType[R]

    val _ = self
      .transform(s"$id/Aggregate") { in =>
        in
          .keyBy(SumByKey[D].key)
          .withFixedWindows(duration = configuration.windowDuration, options = configuration.windowOptions)
          .sumByKey(SumByKey[D].semigroup)
          .values
      }
      .transform(s"$id/Serialize") { in =>
        in
          .withTimestamp
          .map { case (diagnostic, timestamp) => toBigQueryTypeFn(diagnostic, timestamp) }
          .map(bqType.toTableRow)
      }
      .saveAsCustomOutput(id.id, io)
  }
}

trait SCollectionSyntax {

  import scala.language.implicitConversions

  implicit def diagnosticSCollectionOps[D: Coder: SumByKey](sc: SCollection[D]): SCollectionOps[D] =
    new SCollectionOps[D](sc)
}
