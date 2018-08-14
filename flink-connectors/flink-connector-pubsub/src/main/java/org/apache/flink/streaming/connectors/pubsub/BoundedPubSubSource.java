package org.apache.flink.streaming.connectors.pubsub;

import org.apache.flink.api.common.serialization.DeserializationSchema;

import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.pubsub.v1.PubsubMessage;

import java.io.IOException;
import java.util.Optional;

class BoundedPubSubSource<OUT> extends PubSubSource<OUT> {
	private Bound<OUT> bound;

	private Long boundedByAmountOfMessages;
	private Long boundedByTimeSinceLastMessage;

	private BoundedPubSubSource() {
		super();
	}

	protected void setBoundedByAmountOfMessages(Long boundedByAmountOfMessages) {
		this.boundedByAmountOfMessages = boundedByAmountOfMessages;
	}

	protected void setBoundedByTimeSinceLastMessage(Long boundedByTimeSinceLastMessage) {
		this.boundedByTimeSinceLastMessage = boundedByTimeSinceLastMessage;
	}

	private Bound<OUT> createBound() {
		if (boundedByAmountOfMessages != null && boundedByTimeSinceLastMessage != null) {
			return Bound.boundByAmountOfMessagesOrTimeSinceLastMessage(boundedByAmountOfMessages, boundedByTimeSinceLastMessage);
		}

		if (boundedByAmountOfMessages != null) {
			return Bound.boundByAmountOfMessages(boundedByAmountOfMessages);
		}

		if (boundedByTimeSinceLastMessage != null) {
			return Bound.boundByTimeSinceLastMessage(boundedByTimeSinceLastMessage);
		}

		// This is functionally speaking no bound.
		return Bound.boundByAmountOfMessages(Long.MAX_VALUE);
	}

	@Override
	public void run(SourceContext<OUT> sourceContext) {
		bound = createBound();
		bound.start(this);
		super.run(sourceContext);
	}

	@Override
	public void receiveMessage(PubsubMessage message, AckReplyConsumer consumer) {
		super.receiveMessage(message, consumer);
		bound.receivedMessage();
	}

	@SuppressWarnings("unchecked")
	public static <OUT> BoundedPubSubSourceBuilder<OUT, ? extends PubSubSource, ? extends BoundedPubSubSourceBuilder> newBuilder() {
		return new BoundedPubSubSourceBuilder<>(new BoundedPubSubSource<OUT>());
	}

	@SuppressWarnings("unchecked")
	public static class BoundedPubSubSourceBuilder<OUT, PSS extends BoundedPubSubSource<OUT>, BUILDER extends BoundedPubSubSourceBuilder<OUT, PSS, BUILDER>> extends PubSubSourceBuilder<OUT, PSS, BUILDER> {
		private final PSS sourceUnderConstruction;

		public BoundedPubSubSourceBuilder(PSS sourceUnderConstruction) {
			super(sourceUnderConstruction);
			this.sourceUnderConstruction = sourceUnderConstruction;
		}

		public BUILDER boundedByAmountOfMessages(long maxAmountOfMessages) {
			sourceUnderConstruction.setBoundedByAmountOfMessages(maxAmountOfMessages);
			return (BUILDER)this;
		}

		public BUILDER boundedByTimeSinceLastMessage(long timeSinceLastMessage) {
			sourceUnderConstruction.setBoundedByTimeSinceLastMessage(timeSinceLastMessage);
			return (BUILDER)this;
		}

		@Override
		public PSS build() throws IOException {
			return super.build();
		}
	}

}
