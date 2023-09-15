package org.mkuthan.streamprocessing.infrastructure.common

import com.spotify.scio.bigquery.types.BigQueryType

import org.joda.time.Instant

import org.mkuthan.streamprocessing.shared.scio.SumByKey

case class IoDiagnostic(id: String, reason: String, count: Long = 1) {
  private lazy val keyFields = this match {
    case IoDiagnostic(id, reason, count @ _) =>
      Seq(id, reason)
  }
}

object IoDiagnostic {
  @BigQueryType.toTable
  case class Raw(created_at: Instant, id: String, reason: String, count: Long)

  implicit val sumByKey: SumByKey[IoDiagnostic] =
    SumByKey.create(
      keyFn = _.keyFields.mkString("|@|"),
      plusFn = (x, y) => x.copy(count = x.count + y.count)
    )

  def toRaw[T](diagnostic: IoDiagnostic, createdAt: Instant): Raw =
    Raw(
      created_at = createdAt,
      id = diagnostic.id,
      reason = diagnostic.reason,
      count = diagnostic.count
    )
}
