package org.mkuthan.streamprocessing.toll.infrastructure.scio.bigquery

import scala.reflect.runtime.universe.TypeTag
import scala.reflect.ClassTag

import com.spotify.scio.bigquery.types.BigQueryType.HasAnnotation
import com.spotify.scio.coders.Coder
import com.spotify.scio.values.SCollection

import org.mkuthan.streamprocessing.shared.configuration.BigQueryTable

private[bigquery] final class SCollectionOps[T <: HasAnnotation: Coder: ClassTag: TypeTag](
    private val self: SCollection[T]
) {

  import com.spotify.scio.bigquery._

  def saveToBigQuery(table: BigQueryTable[T]): Unit = {
    val _ = self.saveAsTypedBigQueryTable(table.spec)
  }
}

trait SCollectionSyntax {
  import scala.language.implicitConversions

  implicit def bigQuerySCollectionOps[T <: HasAnnotation: Coder: ClassTag: TypeTag](
      sc: SCollection[T]
  ): SCollectionOps[T] =
    new SCollectionOps(sc)
}
