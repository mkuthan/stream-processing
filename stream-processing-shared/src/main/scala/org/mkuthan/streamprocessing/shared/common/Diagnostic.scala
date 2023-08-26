package org.mkuthan.streamprocessing.shared.common

import com.spotify.scio.bigquery.types.BigQueryType

import org.joda.time.Instant

object Diagnostic {

  def apply(createdAt: Instant, id: String, reason: String): Raw =
    Raw(createdAt, id, reason, 1L)

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
        groupKeyFn = _.keyFields.mkString("|@|"),
        plusFn = (x, y) => x.copy(count = x.count + y.count)
      )
  }
}
