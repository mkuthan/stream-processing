package org.mkuthan.streamprocessing.toll.infrastructure.scio.bigquery

import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag
import scala.util.chaining._

import com.spotify.scio.bigquery.types.BigQueryType
import com.spotify.scio.bigquery.types.BigQueryType.HasAnnotation
import com.spotify.scio.coders.Coder
import com.spotify.scio.values.SCollection
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO
import org.mkuthan.streamprocessing.shared.configuration.BigQueryTable

private[bigquery] final class SCollectionOps[T <: HasAnnotation: Coder: ClassTag: TypeTag](
    private val self: SCollection[T]
) {
  def saveToBigQuery(
      table: BigQueryTable[T],
      writeConfiguration: StorageWriteConfiguration = StorageWriteConfiguration()
  ): SCollection[BigQueryDeadLetter[T]] = {
    val bigQueryType = BigQueryType[T]

    val io = BigQueryIO
      .writeTableRows()
      .pipe(write => writeConfiguration.configure(write))
      .to(table.id)

    val pFailedRows = self
      .map(bigQueryType.toTableRow)
      .internal.apply(table.id, io)
      .getFailedStorageApiInserts

    val failedRows = self.context.wrap(pFailedRows)
    failedRows.map { failedRow =>
      val tableRow = failedRow.getRow.toString
      val error = failedRow.getErrorMessage
      BigQueryDeadLetter(tableRow, error)
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
