package org.mkuthan.streamprocessing.shared.scio.bigquery

import org.mkuthan.streamprocessing.shared.scio.common.IoIdentifier

case class BigQueryDeadLetter[T](id: IoIdentifier[T], row: T, error: String)
