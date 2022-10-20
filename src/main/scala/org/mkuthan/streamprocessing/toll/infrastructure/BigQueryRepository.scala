package org.mkuthan.streamprocessing.toll.infrastructure

import scala.reflect.runtime.universe._
import scala.reflect.ClassTag

import com.spotify.scio.bigquery._
import com.spotify.scio.bigquery.types.BigQueryType.HasAnnotation
import com.spotify.scio.coders.Coder
import com.spotify.scio.values.SCollection
import com.spotify.scio.ScioContext

import org.mkuthan.streamprocessing.toll.configuration.BigQueryTable

object BigQueryRepository {
  def load[T <: HasAnnotation: ClassTag: TypeTag: Coder](
      table: BigQueryTable[T]
  )(implicit sc: ScioContext): SCollection[T] =
    sc.typedBigQueryStorage(table.spec)

  def save[T <: HasAnnotation: ClassTag: TypeTag: Coder](
      table: BigQueryTable[T],
      data: SCollection[T]
  ): Unit =
    data.saveAsTypedBigQueryTable(table.spec)
}
