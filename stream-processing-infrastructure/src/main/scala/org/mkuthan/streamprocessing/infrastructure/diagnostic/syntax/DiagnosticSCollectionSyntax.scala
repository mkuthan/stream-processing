package org.mkuthan.streamprocessing.infrastructure.diagnostic.syntax

import scala.reflect.runtime.universe.TypeTag
import scala.reflect.ClassTag

import com.spotify.scio.bigquery.types.BigQueryType.HasAnnotation
import com.spotify.scio.coders.Coder
import com.spotify.scio.values.SCollection

import org.joda.time.Instant

import org.mkuthan.streamprocessing.infrastructure.bigquery.BigQueryDeadLetter
import org.mkuthan.streamprocessing.infrastructure.bigquery.BigQueryPartition
import org.mkuthan.streamprocessing.infrastructure.bigquery.BigQueryTable
import org.mkuthan.streamprocessing.infrastructure.common.IoIdentifier
import org.mkuthan.streamprocessing.infrastructure.diagnostic.DiagnosticConfiguration
import org.mkuthan.streamprocessing.shared.common.SumByKey

private[syntax] trait DiagnosticSCollectionSyntax {

  implicit class DiagnosticSCollectionOps[D: Coder: SumByKey](private val self: SCollection[D]) {

    import DiagnosticSCollectionOps._

    import org.mkuthan.streamprocessing.infrastructure.bigquery.syntax._

    def writeUnboundedDiagnosticToBigQuery[R <: HasAnnotation: Coder: ClassTag: TypeTag](
        id: IoIdentifier[R],
        table: BigQueryTable[R],
        toBigQueryTypeFn: (D, Instant) => R,
        configuration: DiagnosticConfiguration = DiagnosticConfiguration()
    ): SCollection[BigQueryDeadLetter[R]] =
      prepare(self, id, toBigQueryTypeFn, configuration)
        .writeUnboundedToBigQuery(id, table)

    def writeBoundedDiagnosticToBigQuery[R <: HasAnnotation: Coder: ClassTag: TypeTag](
        id: IoIdentifier[R],
        partition: BigQueryPartition[R],
        toBigQueryTypeFn: (D, Instant) => R,
        configuration: DiagnosticConfiguration = DiagnosticConfiguration()
    ): Unit =
      prepare(self, id, toBigQueryTypeFn, configuration)
        .writeBoundedToBigQuery(id, partition)
  }

  private object DiagnosticSCollectionOps {
    private def prepare[D: Coder: SumByKey, R <: HasAnnotation: Coder: ClassTag: TypeTag](
        input: SCollection[D],
        id: IoIdentifier[R],
        toBigQueryTypeFn: (D, Instant) => R,
        configuration: DiagnosticConfiguration
    ): SCollection[R] =
      input
        .transform(s"$id/Aggregate") { in =>
          in
            .keyBy(SumByKey[D].key)
            .withFixedWindows(duration = configuration.windowDuration, options = configuration.windowOptions)
            .sumByKey(SumByKey[D].semigroup)
            .values
        }
        .transform(s"$id/Convert") { in =>
          in
            .withTimestamp
            .map { case (diagnostic, timestamp) => toBigQueryTypeFn(diagnostic, timestamp) }
        }
  }

}
