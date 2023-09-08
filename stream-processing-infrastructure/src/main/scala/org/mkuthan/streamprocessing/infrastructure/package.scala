package org.mkuthan.streamprocessing

package object infrastructure extends bigquery.syntax.BigQuerySyntax
    with diagnostic.syntax.DiagnosticSyntax
    with dlq.syntax.DlqSyntax
    with pubsub.syntax.PubsubSyntax
