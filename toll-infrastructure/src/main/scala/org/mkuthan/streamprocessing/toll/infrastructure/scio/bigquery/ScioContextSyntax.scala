package org.mkuthan.streamprocessing.toll.infrastructure.scio.bigquery

import scala.language.implicitConversions
import scala.reflect.runtime.universe.TypeTag
import scala.reflect.ClassTag

import com.spotify.scio.bigquery._
import com.spotify.scio.bigquery.types.BigQueryType.HasAnnotation
import com.spotify.scio.coders.Coder
import com.spotify.scio.values.SCollection
import com.spotify.scio.ScioContext

import org.mkuthan.streamprocessing.toll.shared.configuration.BigQueryTable

private[bigquery] class ScioContextOps(private val self: ScioContext) extends AnyVal {
  def loadFromBigQuery[T <: HasAnnotation: Coder: ClassTag: TypeTag](
      table: BigQueryTable[T]
  ): SCollection[T] =
    self.typedBigQueryStorage(table.spec)
}

trait ScioContextSyntax {
  implicit def bigQueryScioContextOps(sc: ScioContext): ScioContextOps = new ScioContextOps(sc)
}