package org.mkuthan.streamprocessing

package object infrastructure extends bigquery.syntax.BigQuerySyntax
    with diagnostic.syntax.DiagnosticSyntax
    with pubsub.syntax.PubsubSyntax
    with storage.syntax.StorageSyntax
