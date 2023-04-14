package org.mkuthan.streamprocessing.toll.infrastructure.scio.bigquery

final case class BigQueryDeadLetter[T](
    tableRow: String,
    error: String
)
