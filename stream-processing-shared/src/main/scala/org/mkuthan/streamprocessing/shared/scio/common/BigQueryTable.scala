package org.mkuthan.streamprocessing.shared.scio.common

import org.joda.time.base.BaseLocal
import org.joda.time.format.DateTimeFormat
import org.joda.time.format.DateTimeFormatter
import org.joda.time.LocalDate
import org.joda.time.LocalDateTime

case class BigQueryTable[T](id: String) {

  import org.apache.beam.sdk.io.gcp.bigquery.BigQueryHelpers

  private lazy val spec = BigQueryHelpers.parseTableSpec(id)

  lazy val datasetName: String = spec.getDatasetId
  lazy val tableName: String = spec.getTableId
}

object BigQueryTable {
  private val PartitionDecoratorSeparator = "$"

  private val HourlyPartitionFormat = DateTimeFormat.forPattern("YYYYMMddHH")
  private val DailyPartitionFormat = DateTimeFormat.forPattern("YYYYMMdd")

  def hourlyPartition[T](id: String, dateTime: LocalDateTime): BigQueryTable[T] =
    timePartition(id, dateTime, HourlyPartitionFormat)

  def dailyPartition[T](id: String, date: LocalDate): BigQueryTable[T] =
    timePartition(id, date, DailyPartitionFormat)

  private def timePartition[T](
      id: String,
      baseLocal: BaseLocal,
      dateTimeFormat: DateTimeFormatter
  ): BigQueryTable[T] = {
    val decorator = dateTimeFormat.print(baseLocal)
    BigQueryTable(s"$id$PartitionDecoratorSeparator$decorator")
  }
}
