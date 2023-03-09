package org.mkuthan.streamprocessing.toll.infrastructure

package object scio extends bigquery.ScioContextSyntax
    with bigquery.SCollectionSyntax
    with core.SCollectionSyntax
    with pubsub.ScioContextSyntax
    with pubsub.SCollectionSyntax
    with storage.SCollectionSyntax
