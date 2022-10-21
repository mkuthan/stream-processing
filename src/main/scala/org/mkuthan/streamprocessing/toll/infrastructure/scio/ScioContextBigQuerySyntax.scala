package org.mkuthan.streamprocessing.toll.infrastructure.scio

import scala.language.implicitConversions
import scala.reflect.runtime.universe._
import scala.reflect.ClassTag

import com.spotify.scio.bigquery._
import com.spotify.scio.bigquery.types.BigQueryType.HasAnnotation
import com.spotify.scio.coders.Coder
import com.spotify.scio.values.SCollection
import com.spotify.scio.ScioContext

final class BigQueryScioContextOps(private val self: ScioContext) extends AnyVal {
  def loadFromBigQuery[T <: HasAnnotation: Coder: ClassTag: TypeTag](
      table: BigQueryTable[T]
  ): SCollection[T] =
    self.typedBigQueryStorage(table.spec)
}

@SuppressWarnings(Array("org.wartremover.warts.ImplicitConversion"))
trait ScioContextBigQuerySyntax {
  implicit def bigQueryScioContextOps(sc: ScioContext): BigQueryScioContextOps = new BigQueryScioContextOps(sc)
}
