package org.apache.flink.streaming.connectors.pubsub;

import com.google.cloud.pubsub.v1.Publisher;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.io.LocalCollectionOutputFormat;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.pubsub.emulator.GCloudUnitTestBase;
import org.apache.flink.streaming.connectors.pubsub.emulator.PubsubHelper;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class EmulatedPubSubSourceTest extends GCloudUnitTestBase {

	private final transient Logger LOG = LoggerFactory.getLogger(EmulatedPubSubSourceTest.class);

	private final static String PROJECT_NAME = "FLProject";
	private final static String TOPIC_NAME = "FLTopic";
	private final static String SUBSCRIPTION_NAME = "FLSubscription";

	private static PubsubHelper pubsubHelper;

	@BeforeClass
	public static void setUp() throws Exception {
		pubsubHelper = getPubsubHelper();
		pubsubHelper.createTopic(PROJECT_NAME, TOPIC_NAME);
		pubsubHelper.createSubscription(PROJECT_NAME, SUBSCRIPTION_NAME, PROJECT_NAME, TOPIC_NAME);
	}

	@AfterClass
	public static void tearDown() throws Exception {
		pubsubHelper.deleteSubscription(PROJECT_NAME, SUBSCRIPTION_NAME);
		pubsubHelper.deleteTopic(PROJECT_NAME, TOPIC_NAME);
	}

	@Test
	public void testFlinkSource() throws Exception {
		// Create some messages and put them into pubsub
		List<String> input = Arrays.asList("One", "Two", "Three", "Four", "Five", "Six", "Seven", "Eigth", "Nine", "Ten");

		// Publish the messages into PubSub
		Publisher publisher = pubsubHelper.createPublisher(PROJECT_NAME, TOPIC_NAME);
		input.forEach(s -> {
			try {
				publisher
					.publish(PubsubMessage
						.newBuilder()
						.setData(ByteString.copyFromUtf8(s))
						.build())
					.get();
			} catch (InterruptedException | ExecutionException e) {
				e.printStackTrace();
			}
		});

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.enableCheckpointing(100);

		DataStream<String> fromPubSub = env
			.addSource(BoundedPubSubSource.<String>newBuilder()
				.withDeserializationSchema(new SimpleStringSchema())
				.withProjectSubscriptionName(PROJECT_NAME, SUBSCRIPTION_NAME)
				// Specific for emulator
				.withCredentialsProvider(getPubsubHelper().getCredentialsProvider())
				.withHostAndPort(getPubSubHostPort())
				// Make sure the test topology self terminates
				.boundedByTimeSinceLastMessage(1000)
				.build())
			.name("PubSub source");

		List<String> output = new ArrayList<>(5);
		fromPubSub.writeUsingOutputFormat(new LocalCollectionOutputFormat<>(output));

		env.execute();

		assertEquals("Wrong number of elements", input.size(), output.size());
		for (String test : input) {
			assertTrue("Missing " + test, output.contains(test));
		}
	}

}
