package org.mkuthan.streamprocessing.toll.infrastructure

import com.spotify.scio.ScioContext
import com.spotify.scio.bigquery.Table
import com.spotify.scio.bigquery.types.BigQueryType.HasAnnotation
import com.spotify.scio.values.SCollection
import org.mkuthan.streamprocessing.toll.configuration.BigQueryTable

object BigQueryRepository {
  def load[T <: HasAnnotation](table: BigQueryTable[T])(implicit sc: ScioContext): SCollection[T] =
    sc.typedBigQueryStorage(Table.Spec(table.id))

  def save[T <: HasAnnotation](table: BigQueryTable[T], data: SCollection[T]): Unit =
    data.saveAsTypedBigQueryTable(Table.Spec(table.id))
}
