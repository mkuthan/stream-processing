package org.mkuthan.streamprocessing.toll.infrastructure.scio

import scala.language.implicitConversions
import scala.reflect.runtime.universe._
import scala.reflect.ClassTag

import com.spotify.scio.bigquery.types.BigQueryType.HasAnnotation
import com.spotify.scio.coders.Coder
import com.spotify.scio.values.SCollection

final class BigQuerySCollectionOps[T <: HasAnnotation](private val self: SCollection[T])
    extends AnyVal {

  import com.spotify.scio.bigquery._

  def saveToBigQuery(table: BigQueryTable[T])(implicit c: Coder[T], ct: ClassTag[T], tt: TypeTag[T]): Unit = {
    self.saveAsTypedBigQueryTable(table.spec)
  }
}

@SuppressWarnings(Array("org.wartremover.warts.ImplicitConversion"))
trait SCollectionBigQuerySyntax {
  implicit def bigQuerySCollectionOps[T <: HasAnnotation](sc: SCollection[T]): BigQuerySCollectionOps[T] =
    new BigQuerySCollectionOps(sc)
}
