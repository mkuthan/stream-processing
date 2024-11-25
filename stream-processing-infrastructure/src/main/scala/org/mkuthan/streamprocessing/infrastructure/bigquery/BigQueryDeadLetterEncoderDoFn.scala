package org.mkuthan.streamprocessing.infrastructure.bigquery

import scala.reflect.runtime.universe.TypeTag
import scala.reflect.ClassTag

import org.apache.beam.sdk.io.gcp.bigquery.BigQueryStorageApiInsertError
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.DoFn.Element
import org.apache.beam.sdk.transforms.DoFn.OutputReceiver
import org.apache.beam.sdk.transforms.DoFn.ProcessElement

import com.spotify.scio.bigquery.types.BigQueryType
import com.spotify.scio.bigquery.types.BigQueryType.HasAnnotation
import com.spotify.scio.coders.Coder

private[bigquery] object BigQueryDeadLetterEncoderDoFn {
  private type In = BigQueryStorageApiInsertError
  private type Out[T] = BigQueryDeadLetter[T]
}

private[bigquery] class BigQueryDeadLetterEncoderDoFn[T <: HasAnnotation: Coder: ClassTag: TypeTag]
    extends DoFn[BigQueryDeadLetterEncoderDoFn.In, BigQueryDeadLetterEncoderDoFn.Out[T]] {

  import BigQueryDeadLetterEncoderDoFn._

  @transient
  private lazy val bigQueryType = BigQueryType[T]

  @ProcessElement
  def processElement(
      @Element element: In,
      output: OutputReceiver[Out[T]]
  ): Unit = {
    val row = bigQueryType.fromTableRow(element.getRow)
    val deadLetter = BigQueryDeadLetter(row, element.getErrorMessage)
    output.output(deadLetter)
  }
}
