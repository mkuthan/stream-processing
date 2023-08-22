package org.mkuthan.streamprocessing.shared.common

import com.spotify.scio.bigquery.types.BigQueryType
import com.spotify.scio.values.SCollection

import com.twitter.algebird.Semigroup

object Diagnostic {

  def apply(id: String, reason: String, count: Long = 1L): Diagnostic =
    Diagnostic(id, reason, count)

  type DiagnosticKey = String

  @BigQueryType.toTable
  case class Diagnostic(id: String, reason: String, count: Long) {
    lazy val key: DiagnosticKey = id + reason
  }

  implicit object DiagnosticSemigroup extends Semigroup[Diagnostic] {
    override def plus(x: Diagnostic, y: Diagnostic): Diagnostic = {
      require(x.key == y.key)
      Diagnostic(x.id, x.reason, x.count + y.count)
    }
  }

  def unionAll(diagnostics: SCollection[Diagnostic]*): SCollection[(DiagnosticKey, Diagnostic)] =
    SCollection
      .unionAll(diagnostics)
      .keyBy(_.key)
}
