package org.mkuthan.streamprocessing.infrastructure.bigquery

import org.joda.time.format.DateTimeFormat
import org.joda.time.LocalDate
import org.joda.time.LocalDateTime

final case class BigQueryPartition[T](id: String) {

  import org.apache.beam.sdk.io.gcp.bigquery.BigQueryHelpers

  private lazy val spec = BigQueryHelpers.parseTableSpec(id)

  lazy val datasetName: String = spec.getDatasetId
  lazy val tableName: String = spec.getTableId
}

object BigQueryPartition {
  private val DecoratorSeparator = "$"

  private val HourlyFormat = DateTimeFormat.forPattern("YYYYMMddHH")
  private val DailyFormat = DateTimeFormat.forPattern("YYYYMMdd")

  def notPartitioned[T](id: String): BigQueryPartition[T] =
    BigQueryPartition(id)

  def hourly[T](id: String, dateTime: LocalDateTime): BigQueryPartition[T] =
    BigQueryPartition(tableWithDecorator(id, HourlyFormat.print(dateTime)))

  def daily[T](id: String, date: LocalDate): BigQueryPartition[T] =
    BigQueryPartition(tableWithDecorator(id, DailyFormat.print(date)))

  private def tableWithDecorator(id: String, decorator: String): String =
    s"$id$DecoratorSeparator$decorator"

}
