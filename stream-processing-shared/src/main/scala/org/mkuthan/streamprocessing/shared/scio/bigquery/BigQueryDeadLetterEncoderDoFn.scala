package org.mkuthan.streamprocessing.shared.scio.bigquery

import scala.annotation.unused
import scala.reflect.runtime.universe.TypeTag
import scala.reflect.ClassTag

import com.spotify.scio.bigquery.types.BigQueryType
import com.spotify.scio.bigquery.types.BigQueryType.HasAnnotation
import com.spotify.scio.coders.Coder

import com.google.api.services.bigquery.model.TableRow
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.DoFn.Element
import org.apache.beam.sdk.transforms.DoFn.OutputReceiver
import org.apache.beam.sdk.transforms.DoFn.ProcessElement

private[bigquery] object BigQueryDeadLetterEncoderDoFn {
  private type In = (TableRow, String)
  private type Out[T] = BigQueryDeadLetter[T]
}

private[bigquery] class BigQueryDeadLetterEncoderDoFn[T <: HasAnnotation: Coder: ClassTag: TypeTag]
    extends DoFn[BigQueryDeadLetterEncoderDoFn.In, BigQueryDeadLetterEncoderDoFn.Out[T]]() {

  import BigQueryDeadLetterEncoderDoFn._

  @transient
  private lazy val bigQueryType = BigQueryType[T]

  @ProcessElement
  @unused
  def processElement(
      @Element element: In,
      output: OutputReceiver[Out[T]]
  ): Unit =
    element match {
      case (tableRow, error) =>
        val row = bigQueryType.fromTableRow(tableRow)
        val deadLetter = BigQueryDeadLetter(row, error)
        output.output(deadLetter)
    }
}
