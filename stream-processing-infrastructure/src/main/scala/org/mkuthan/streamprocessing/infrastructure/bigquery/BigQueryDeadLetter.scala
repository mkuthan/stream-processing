package org.mkuthan.streamprocessing.infrastructure.bigquery

case class BigQueryDeadLetter[T](
    row: T,
    error: String
)
