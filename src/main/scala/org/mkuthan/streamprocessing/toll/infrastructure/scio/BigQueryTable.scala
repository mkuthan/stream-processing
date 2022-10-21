package org.mkuthan.streamprocessing.toll.infrastructure.scio

import com.spotify.scio.bigquery.Table

final case class BigQueryTable[T](id: String) extends AnyVal {
  def spec: Table.Spec = Table.Spec(id)
}
