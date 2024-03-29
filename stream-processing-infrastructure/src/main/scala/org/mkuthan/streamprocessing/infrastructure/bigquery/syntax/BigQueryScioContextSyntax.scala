package org.mkuthan.streamprocessing.infrastructure.bigquery.syntax

import scala.reflect.runtime.universe.TypeTag
import scala.reflect.ClassTag
import scala.util.chaining.scalaUtilChainingOps

import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO

import com.spotify.scio.bigquery.types.BigQueryType
import com.spotify.scio.bigquery.types.BigQueryType.HasAnnotation
import com.spotify.scio.coders.Coder
import com.spotify.scio.values.SCollection
import com.spotify.scio.ScioContext

import org.mkuthan.streamprocessing.infrastructure.bigquery.BigQueryQuery
import org.mkuthan.streamprocessing.infrastructure.bigquery.BigQueryTable
import org.mkuthan.streamprocessing.infrastructure.bigquery.ExportConfiguration
import org.mkuthan.streamprocessing.infrastructure.bigquery.StorageReadConfiguration
import org.mkuthan.streamprocessing.infrastructure.common.IoIdentifier

private[syntax] trait BigQueryScioContextSyntax {

  implicit class BigQueryScioContextOps(private val self: ScioContext) {

    import com.spotify.scio.TestableScioContext._

    def queryFromBigQuery[T <: HasAnnotation: Coder: ClassTag: TypeTag](
        id: IoIdentifier[T],
        query: BigQueryQuery[T],
        configuration: ExportConfiguration = ExportConfiguration()
    ): SCollection[T] = {
      val io = BigQueryIO
        .readTableRows()
        .pipe(read => configuration.configure(read))
        .fromQuery(query.query)

      val bigQueryType = BigQueryType[T]

      self.testableCustomInput(id.id) { in =>
        self.wrap(in.apply("Query", io))
          .withName("Deserialize")
          .map(bigQueryType.fromTableRow)
      }
    }

    def readFromBigQuery[T <: HasAnnotation: Coder: ClassTag: TypeTag](
        id: IoIdentifier[T],
        table: BigQueryTable[T],
        configuration: StorageReadConfiguration = StorageReadConfiguration()
    ): SCollection[T] = {
      val io = BigQueryIO
        .readTableRows()
        .pipe(read => configuration.configure(read))
        .from(table.id)

      val bigQueryType = BigQueryType[T]

      self.testableCustomInput(id.id) { in =>
        self.wrap(in.apply("Read", io))
          .withName("Deserialize")
          .map(bigQueryType.fromTableRow)
      }
    }
  }

}
