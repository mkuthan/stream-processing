package org.mkuthan.streamprocessing

package object infrastructure extends bigquery.ScioContextSyntax
    with bigquery.SCollectionSyntax
    with diagnostic.SCollectionSyntax
    with dlq.SCollectionSyntax
    with pubsub.ScioContextSyntax
    with pubsub.SCollectionSyntax
    with pubsub.PubsubCoders
