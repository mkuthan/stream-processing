package org.mkuthan.streamprocessing.toll.infrastructure.scio.bigquery

import scala.reflect.runtime.universe.TypeTag
import scala.reflect.ClassTag

import com.spotify.scio.bigquery.types.BigQueryType.HasAnnotation
import com.spotify.scio.coders.Coder
import com.spotify.scio.values.SCollection

import org.mkuthan.streamprocessing.toll.shared.configuration.BigQueryTable

private[bigquery] final class SCollectionOps[T <: HasAnnotation](private val self: SCollection[T])
    extends AnyVal {

  import com.spotify.scio.bigquery._

  def saveToBigQuery(table: BigQueryTable[T])(implicit c: Coder[T], ct: ClassTag[T], tt: TypeTag[T]): Unit = {
    val _ = self.saveAsTypedBigQueryTable(table.spec)
  }
}

trait SCollectionSyntax {
  import scala.language.implicitConversions

  implicit def bigQuerySCollectionOps[T <: HasAnnotation](sc: SCollection[T]): SCollectionOps[T] =
    new SCollectionOps(sc)
}
