package org.mkuthan.streamprocessing.infrastructure.diagnostic

import com.spotify.scio.bigquery.types.BigQueryType

import org.joda.time.Instant

import org.mkuthan.streamprocessing.infrastructure.common.IoIdentifier
import org.mkuthan.streamprocessing.shared.common.SumByKey

object IoDiagnostic {

  def apply[T](createdAt: Instant, id: IoIdentifier[T], reason: String): Raw =
    Raw(createdAt, id.id, reason, 1L)

  @BigQueryType.toTable
  case class Raw(created_at: Instant, id: String, reason: String, count: Long) {
    private lazy val keyFields = this match {
      case Raw(created_at, id, reason, count @ _) =>
        Seq(created_at, id, reason)
    }
  }

  object Raw {
    implicit val diagnostic: SumByKey[Raw] =
      SumByKey.create(
        keyFn = _.keyFields.mkString("|@|"),
        plusFn = (x, y) => x.copy(count = x.count + y.count)
      )
  }
}
