package org.mkuthan.streamprocessing.shared.scio.common

import org.joda.time.format.DateTimeFormat
import org.joda.time.LocalDate
import org.joda.time.LocalDateTime

case class BigQueryPartition[T](id: String, decorator: Option[String]) {

  import org.apache.beam.sdk.io.gcp.bigquery.BigQueryHelpers
  import BigQueryPartition._

  private lazy val idWithDecorator = decorator
    .map(decorator => s"$id$DecoratorSeparator$decorator")
    .getOrElse(id)

  private lazy val spec = BigQueryHelpers.parseTableSpec(idWithDecorator)

  lazy val datasetName: String = spec.getDatasetId
  lazy val tableName: String = spec.getTableId
}

object BigQueryPartition {
  private val DecoratorSeparator = "$"

  private val HourlyFormat = DateTimeFormat.forPattern("YYYYMMddHH")
  private val DailyFormat = DateTimeFormat.forPattern("YYYYMMdd")

  def notPartitioned[T](id: String): BigQueryPartition[T] =
    BigQueryPartition(id, None)

  def hourly[T](id: String, dateTime: LocalDateTime): BigQueryPartition[T] =
    BigQueryPartition(id, Some(HourlyFormat.print(dateTime)))

  def daily[T](id: String, date: LocalDate): BigQueryPartition[T] =
    BigQueryPartition(id, Some(DailyFormat.print(date)))

}
