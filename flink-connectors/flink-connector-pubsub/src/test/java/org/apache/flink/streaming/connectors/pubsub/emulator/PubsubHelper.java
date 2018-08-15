package org.apache.flink.streaming.connectors.pubsub.emulator;

import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.rpc.NotFoundException;
import com.google.api.gax.rpc.TransportChannelProvider;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.cloud.pubsub.v1.SubscriptionAdminClient;
import com.google.cloud.pubsub.v1.SubscriptionAdminSettings;
import com.google.cloud.pubsub.v1.TopicAdminClient;
import com.google.cloud.pubsub.v1.TopicAdminSettings;
import com.google.cloud.pubsub.v1.stub.GrpcSubscriberStub;
import com.google.cloud.pubsub.v1.stub.SubscriberStub;
import com.google.cloud.pubsub.v1.stub.SubscriberStubSettings;
import com.google.pubsub.v1.AcknowledgeRequest;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.ProjectTopicName;
import com.google.pubsub.v1.PullRequest;
import com.google.pubsub.v1.PullResponse;
import com.google.pubsub.v1.PushConfig;
import com.google.pubsub.v1.ReceivedMessage;
import com.google.pubsub.v1.Topic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class PubsubHelper {

    private static final Logger LOG = LoggerFactory.getLogger(PubsubHelper.class);

    private TransportChannelProvider channelProvider     = null;
    private CredentialsProvider      credentialsProvider = null;

    private TopicAdminClient        topicClient;
    private SubscriptionAdminClient subscriptionAdminClient;

    public PubsubHelper() {
        this(TopicAdminSettings.defaultTransportChannelProvider(),
             TopicAdminSettings.defaultCredentialsProviderBuilder().build());
    }

    public PubsubHelper(TransportChannelProvider channelProvider, CredentialsProvider credentialsProvider) {
        this.channelProvider = channelProvider;
        this.credentialsProvider = credentialsProvider;
    }

    public TransportChannelProvider getChannelProvider() {
        return channelProvider;
    }

    public CredentialsProvider getCredentialsProvider() {
        return credentialsProvider;
    }

    public TopicAdminClient getTopicAdminClient() throws IOException {
        if(topicClient == null) {
            TopicAdminSettings topicAdminSettings = TopicAdminSettings.newBuilder()
                    .setTransportChannelProvider(channelProvider)
                    .setCredentialsProvider(credentialsProvider)
                    .build();
            topicClient = TopicAdminClient.create(topicAdminSettings);
        }
        return topicClient;
    }

    public Topic createTopic(String project, String topic) throws IOException {
        deleteTopic(project, topic);
        ProjectTopicName topicName   = ProjectTopicName.of(project, topic);
        TopicAdminClient adminClient = getTopicAdminClient();
        LOG.info("createTopic {}", topicName);
        return adminClient.createTopic(topicName);
    }

    public void deleteTopic(String project, String topic) throws IOException {
        deleteTopic(ProjectTopicName.of(project, topic));
    }

    public void deleteTopic(ProjectTopicName topicName) throws IOException {
//        LOG.info("CreateTopic {}", topicName);
        TopicAdminClient adminClient = getTopicAdminClient();
        try {
            Topic existingTopic = adminClient.getTopic(topicName);

            // If it exists we delete all subscriptions and the topic itself.
            LOG.info("deleteTopic {} first delete old subscriptions.", topicName);
            adminClient
                    .listTopicSubscriptions(topicName)
                    .iterateAllAsProjectSubscriptionName()
                    .forEach(subscriptionAdminClient::deleteSubscription);
            LOG.info("deleteTopic {}", topicName);
            adminClient
                    .deleteTopic(topicName);
        } catch (NotFoundException e) {
            // Doesn't exist. Good.
        }
    }

    public SubscriptionAdminClient getSubscriptionAdminClient() throws IOException {
        if(subscriptionAdminClient == null) {
            SubscriptionAdminSettings subscriptionAdminSettings =
                    SubscriptionAdminSettings
                    .newBuilder()
                    .setTransportChannelProvider(channelProvider)
                    .setCredentialsProvider(credentialsProvider)
                    .build();
            subscriptionAdminClient = SubscriptionAdminClient.create(subscriptionAdminSettings);
        }
        return subscriptionAdminClient;
    }

    public void createSubscription(String subscriptionProject, String subscription, String topicProject, String topic) throws IOException {
        ProjectSubscriptionName subscriptionName = ProjectSubscriptionName.newBuilder()
                .setProject(subscriptionProject)
                .setSubscription(subscription)
                .build();

        deleteSubscription(subscriptionName);

        SubscriptionAdminClient adminClient = getSubscriptionAdminClient();

        ProjectTopicName topicName = ProjectTopicName.of(topicProject, topic);

        PushConfig pushConfig =  PushConfig.getDefaultInstance();

        LOG.info("createSubscription {}", subscriptionName);
        getSubscriptionAdminClient().createSubscription(subscriptionName, topicName, pushConfig, 1);
    }

    public void deleteSubscription(String subscriptionProject, String subscription) throws IOException {
        deleteSubscription(ProjectSubscriptionName
                .newBuilder()
                .setProject(subscriptionProject)
                .setSubscription(subscription)
                .build());
    }

    public void deleteSubscription(ProjectSubscriptionName subscriptionName) throws IOException {
        SubscriptionAdminClient adminClient = getSubscriptionAdminClient();
        try {
            adminClient.getSubscription(subscriptionName);
            // If it already exists we must first delete it.
            LOG.info("deleteSubscription {}", subscriptionName);
            adminClient.deleteSubscription(subscriptionName);
        } catch (NotFoundException e) {
            // Doesn't exist. Good.
        }
    }

    // Mostly copied from the example on https://cloud.google.com/pubsub/docs/pull
    public List<ReceivedMessage> pullMessages(String projectId, String subscriptionId, int maxNumberOfMessages) throws Exception {
        SubscriberStubSettings subscriberStubSettings =
                SubscriberStubSettings.newBuilder()
                        .setTransportChannelProvider(channelProvider)
                        .setCredentialsProvider(credentialsProvider)
                        .build();
        try (SubscriberStub subscriber = GrpcSubscriberStub.create(subscriberStubSettings)) {
            // String projectId = "my-project-id";
            // String subscriptionId = "my-subscription-id";
            // int numOfMessages = 10;   // max number of messages to be pulled
            String subscriptionName = ProjectSubscriptionName.format(projectId, subscriptionId);
            PullRequest pullRequest =
                    PullRequest.newBuilder()
                            .setMaxMessages(maxNumberOfMessages)
                            .setReturnImmediately(false) // return immediately if messages are not available
                            .setSubscription(subscriptionName)
                            .build();

            // use pullCallable().futureCall to asynchronously perform this operation
            PullResponse pullResponse = subscriber.pullCallable().call(pullRequest);
            List<String> ackIds       = new ArrayList<>();
            for (ReceivedMessage message : pullResponse.getReceivedMessagesList()) {
                // handle received message
                // ...
                ackIds.add(message.getAckId());
            }
            // acknowledge received messages
            AcknowledgeRequest acknowledgeRequest =
                    AcknowledgeRequest.newBuilder()
                            .setSubscription(subscriptionName)
                            .addAllAckIds(ackIds)
                            .build();
            // use acknowledgeCallable().futureCall to asynchronously perform this operation
            subscriber.acknowledgeCallable().call(acknowledgeRequest);
            return pullResponse.getReceivedMessagesList();
        }
    }

    public Subscriber subscribeToSubscription(String project, String subscription, MessageReceiver messageReceiver) {
        ProjectSubscriptionName subscriptionName = ProjectSubscriptionName.of(project, subscription);
        Subscriber subscriber =
                Subscriber
                        .newBuilder(subscriptionName, messageReceiver)
                        .setChannelProvider(channelProvider)
                        .setCredentialsProvider(credentialsProvider)
                        .build();
        subscriber.startAsync();
        return subscriber;
    }


    public Publisher createPublisher(String project, String topic) throws IOException {
        return Publisher
                .newBuilder(ProjectTopicName.of(project, topic))
                .setChannelProvider(channelProvider)
                .setCredentialsProvider(credentialsProvider)
                .build();
    }

}
