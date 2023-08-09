package org.mkuthan.streamprocessing.toll.domain.common

import com.spotify.scio.bigquery.types.BigQueryType

import com.twitter.algebird.Semigroup

import org.mkuthan.streamprocessing.shared.scio.common.IoIdentifier

object IoDiagnostic {

  def apply[T](id: IoIdentifier[T], reason: String, count: Long = 1L): Diagnostic =
    Diagnostic(id.id, reason, count)

  @BigQueryType.toTable
  case class Diagnostic(id: String, reason: String, count: Long) {
    lazy val key: String = id + reason
  }

  implicit object DiagnosticSemigroup extends Semigroup[Diagnostic] {
    override def plus(x: Diagnostic, y: Diagnostic): Diagnostic = {
      require(x.key == y.key)
      Diagnostic(x.id, x.reason, x.count + y.count)
    }
  }
}
