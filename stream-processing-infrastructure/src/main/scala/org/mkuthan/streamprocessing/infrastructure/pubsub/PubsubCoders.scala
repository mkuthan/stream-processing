package org.mkuthan.streamprocessing.infrastructure.pubsub

import org.apache.beam.sdk.io.gcp.pubsub.{PubsubMessage => BeamPubsubMessage}
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessageWithAttributesCoder

import com.spotify.scio.coders.Coder

trait PubsubCoders {
  implicit def pubsubMessageCoder: Coder[BeamPubsubMessage] =
    Coder.beam(PubsubMessageWithAttributesCoder.of())
}
