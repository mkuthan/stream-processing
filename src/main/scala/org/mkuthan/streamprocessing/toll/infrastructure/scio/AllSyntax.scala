package org.mkuthan.streamprocessing.toll.infrastructure.scio

trait AllSyntax
    extends ScioContextBigQuerySyntax
    with ScioContextPubSubSyntax
    with SCollectionBigQuerySyntax
    with SCollectionPubSubSyntax
    with SCollectionStorageSyntax
