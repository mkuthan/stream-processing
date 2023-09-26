package org.mkuthan.streamprocessing.infrastructure.bigquery

final case class BigQueryDeadLetter[T](
    row: T,
    error: String
)
