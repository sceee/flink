package org.apache.flink.streaming.connectors.pubsub;

import com.google.cloud.pubsub.v1.Publisher;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.ReceivedMessage;
import org.apache.flink.streaming.connectors.pubsub.emulator.GCloudUnitTestBase;
import org.apache.flink.streaming.connectors.pubsub.emulator.PubsubHelper;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeoutException;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.Assert.assertEquals;

public class DockerBasedPubSubTest extends GCloudUnitTestBase {

    private final transient Logger LOG = LoggerFactory.getLogger(DockerBasedPubSubTest.class);

    private final static String PROJECT_NAME      = "Project";
    private final static String TOPIC_NAME        = "Topic";
    private final static String SUBSCRIPTION_NAME = "Subscription";

    private static PubsubHelper pubsubHelper = getPubsubHelper();

    @BeforeClass
    public static void setUp() throws Exception {
        pubsubHelper.createTopic(PROJECT_NAME, TOPIC_NAME);
        pubsubHelper.createSubscription(PROJECT_NAME, SUBSCRIPTION_NAME, PROJECT_NAME, TOPIC_NAME);
    }

    @AfterClass
    public static void tearDown() throws Exception {
        pubsubHelper.deleteSubscription(PROJECT_NAME, SUBSCRIPTION_NAME);
        pubsubHelper.deleteTopic(PROJECT_NAME, TOPIC_NAME);
    }

    @Test
    public void testPull() throws Exception {
        Publisher publisher = pubsubHelper.createPublisher(PROJECT_NAME, TOPIC_NAME);
        publisher
                .publish(PubsubMessage
                        .newBuilder()
                        .setData(ByteString.copyFromUtf8("Hello World PULL"))
                        .build())
                .get();

        List<ReceivedMessage> receivedMessages = pubsubHelper.pullMessages(PROJECT_NAME, SUBSCRIPTION_NAME, 10);
        assertEquals(1, receivedMessages.size());
        assertEquals("Hello World PULL", receivedMessages.get(0).getMessage().getData().toStringUtf8());

        publisher.shutdown();
    }

    @Test
    public void testPush() throws Exception {
        List<PubsubMessage> receivedMessages = new ArrayList<>();
        Subscriber subscriber = pubsubHelper.
            subscribeToSubscription(
                PROJECT_NAME,
                SUBSCRIPTION_NAME,
                (message, consumer) -> receivedMessages.add(message)
            );

        Publisher publisher = pubsubHelper.createPublisher(PROJECT_NAME, TOPIC_NAME);
        publisher
            .publish(PubsubMessage
                    .newBuilder()
                    .setData(ByteString.copyFromUtf8("Hello World"))
                    .build())
            .get();

        LOG.info("Waiting a while to receive the message...");
        Thread.sleep(1000);

        assertEquals(1, receivedMessages.size());
        assertEquals("Hello World", receivedMessages.get(0).getData().toStringUtf8());

        try {
            subscriber.stopAsync().awaitTerminated(100, MILLISECONDS);
        } catch (TimeoutException tme) {
            // Yeah, whatever. Don't care about clean shutdown here.
        }
        publisher.shutdown();
    }

}
