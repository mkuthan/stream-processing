package org.mkuthan.streamprocessing

package object infrastructure extends bigquery.BigQueryScioContextSyntax
    with bigquery.BigQuerySCollectionSyntax
    with diagnostic.DiagnosticSCollectionSyntax
    with dlq.DlqSCollectionSyntax
    with pubsub.PubsubScioContextSyntax
    with pubsub.PubsubSCollectionSyntax
    with pubsub.PubsubCoders
