package org.mkuthan.streamprocessing

package object infrastructure extends bigquery.syntax.BigQuerySyntax
    with pubsub.syntax.PubsubSyntax
    with storage.syntax.StorageSyntax
