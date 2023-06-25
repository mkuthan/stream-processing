package org.mkuthan.streamprocessing.shared.scio.bigquery

case class BigQueryDeadLetter[T](
    // row: T,
    error: String
)
