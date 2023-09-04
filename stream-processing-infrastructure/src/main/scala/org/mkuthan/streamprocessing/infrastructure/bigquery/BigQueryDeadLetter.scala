package org.mkuthan.streamprocessing.infrastructure.bigquery

import org.mkuthan.streamprocessing.infrastructure.common.IoIdentifier

case class BigQueryDeadLetter[T](
    id: IoIdentifier[T],
    row: T,
    error: String
)
