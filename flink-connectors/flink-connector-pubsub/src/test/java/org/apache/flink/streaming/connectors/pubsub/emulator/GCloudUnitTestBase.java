package org.apache.flink.streaming.connectors.pubsub.emulator;

import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.grpc.GrpcTransportChannel;
import com.google.api.gax.rpc.FixedTransportChannelProvider;
import com.google.api.gax.rpc.TransportChannelProvider;
import com.spotify.docker.client.exceptions.DockerException;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.junit.AfterClass;
import org.junit.BeforeClass;


import java.io.Serializable;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.flink.streaming.connectors.pubsub.emulator.GCloudEmulatorManager.getDockerIpAddress;
import static org.apache.flink.streaming.connectors.pubsub.emulator.GCloudEmulatorManager.getDockerPubSubPort;

public class GCloudUnitTestBase implements Serializable {
    @BeforeClass
    public static void launchGCloudEmulator() throws Exception {
        // Separated out into separate class because Beam needs the entire test class to be serializable
        GCloudEmulatorManager.launchDocker();
    }

    @AfterClass
    public static void terminateGCloudEmulator() throws DockerException, InterruptedException {
        GCloudEmulatorManager.terminateDocker();
    }

    // ====================================================================================
    // DataStore helpers

//    public static DatastoreOptions getDatastoreOptions() {
//        return DatastoreOptions
//                .newBuilder()
//                .setHost("http://" + getDockerIpAddress() + ":" + getDockerDataStorePort())
//                .setProjectId(UNITTEST_PROJECT_ID)
//                .setCredentials(NoCredentials.getInstance())
//                .build();
//    }

    // ====================================================================================
    // Pubsub helpers

    private static ManagedChannel           channel             = null;
    private static TransportChannelProvider channelProvider     = null;
    private static CredentialsProvider      credentialsProvider = null;

    public static PubsubHelper getPubsubHelper() {
        if (channel == null) {
            channel = ManagedChannelBuilder
                    .forTarget(getPubSubHostPort())
                    .usePlaintext(true)
                    .build();
            channelProvider = FixedTransportChannelProvider
                    .create(GrpcTransportChannel.create(channel));
            credentialsProvider = NoCredentialsProvider.create();
        }
        return new PubsubHelper(channelProvider, credentialsProvider);
    }

    public static String getPubSubHostPort() {
        return getDockerIpAddress() + ":" + getDockerPubSubPort();
    }

    @AfterClass
    public static void cleanupPubsubChannel() throws InterruptedException {
        if (channel != null) {
            channel.shutdownNow().awaitTermination(1, SECONDS);
            channel = null;
        }
    }

    // ====================================================================================
    // BigTable helpers

    // TODO: The same for BigTable
}
