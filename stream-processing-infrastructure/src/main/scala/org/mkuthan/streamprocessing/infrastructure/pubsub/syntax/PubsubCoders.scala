package org.mkuthan.streamprocessing.infrastructure.pubsub.syntax

import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessageWithAttributesCoder

import com.spotify.scio.coders.Coder

private[syntax] trait PubsubCoders {
  implicit def pubsubMessageWithAttributesCoder: Coder[PubsubMessage] =
    Coder.beam(PubsubMessageWithAttributesCoder.of())
}
