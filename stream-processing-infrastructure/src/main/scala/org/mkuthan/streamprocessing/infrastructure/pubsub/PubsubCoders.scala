package org.mkuthan.streamprocessing.infrastructure.pubsub

import com.spotify.scio.coders.Coder

import org.apache.beam.sdk.io.gcp.pubsub.{PubsubMessage => BeamPubsubMessage}
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessageWithAttributesCoder

trait PubsubCoders {
  implicit def pubsubMessageCoder: Coder[BeamPubsubMessage] =
    Coder.beam(PubsubMessageWithAttributesCoder.of())
}
