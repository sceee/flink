package org.apache.flink.streaming.connectors.pubsub;

import org.apache.flink.api.common.serialization.DeserializationSchema;

import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.pubsub.v1.PubsubMessage;

class BoundedPubSubSource<OUT> extends PubSubSource<OUT> {
	private static Bound bound;

	BoundedPubSubSource(SubscriberWrapper subscriberWrapper, DeserializationSchema<OUT> deserializationSchema, Bound<OUT> bound) {
		super(subscriberWrapper, deserializationSchema);
		BoundedPubSubSource.bound = bound;
	}

	@Override
	public void run(SourceContext<OUT> sourceContext) {
		bound.start(this);
		super.run(sourceContext);
	}

	@Override
	public void receiveMessage(PubsubMessage message, AckReplyConsumer consumer) {
		super.receiveMessage(message, consumer);
		bound.receivedMessage();
	}
}
