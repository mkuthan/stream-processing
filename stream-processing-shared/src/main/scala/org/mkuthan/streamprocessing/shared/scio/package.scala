package org.mkuthan.streamprocessing.shared

package object scio extends bigquery.ScioContextSyntax
    with bigquery.SCollectionSyntax
    with core.SCollectionSyntax
    with pubsub.ScioContextSyntax
    with pubsub.SCollectionSyntax
    with pubsub.PubsubCoders
    with storage.SCollectionSyntax