package org.mkuthan.streamprocessing.toll.infrastructure.scio.bigquery

import scala.language.implicitConversions
import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag
import scala.util.chaining._

import com.spotify.scio.ScioContext
import com.spotify.scio.bigquery.types.BigQueryType
import com.spotify.scio.bigquery.types.BigQueryType.HasAnnotation
import com.spotify.scio.coders.Coder
import com.spotify.scio.values.SCollection
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.TypedRead
import org.mkuthan.streamprocessing.shared.configuration.BigQueryTable

private[bigquery] class ScioContextOps(private val self: ScioContext) extends AnyVal {
  def loadFromBigQuery[T <: HasAnnotation: Coder: ClassTag: TypeTag](
      table: BigQueryTable[T],
      readConfiguration: StorageReadConfiguration = StorageReadConfiguration()
  ): SCollection[T] = {
    val io = BigQueryIO
      .readTableRows()
      .withMethod(TypedRead.Method.DIRECT_READ)
      .pipe(read => readConfiguration.configure(read))
      .from(table.id)

    val bigQueryType = BigQueryType[T]

    self
      .customInput(table.id, io)
      .map(bigQueryType.fromTableRow)
  }
}

trait ScioContextSyntax {
  implicit def bigQueryScioContextOps(sc: ScioContext): ScioContextOps = new ScioContextOps(sc)
}
