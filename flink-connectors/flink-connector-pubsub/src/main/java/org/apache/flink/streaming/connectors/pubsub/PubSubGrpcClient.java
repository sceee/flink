/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.pubsub;

import com.google.pubsub.v1.AcknowledgeRequest;
import com.google.pubsub.v1.SubscriberGrpc;
import io.grpc.Channel;
import io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.shaded.io.grpc.netty.NegotiationType;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;

import java.io.IOException;
import java.util.List;

class PubSubGrpcClient {
	private static final String PUBSUB_ADDRESS = "pubsub.googleapis.com";
	private static final int PUBSUB_PORT = 443;
	private String projectId;
	private String subscriptionId;

	private transient SubscriberGrpc.SubscriberBlockingStub subscriberStub;

	PubSubGrpcClient(String projectId, String subscriptionId) {
		this.projectId = projectId;
		this.subscriptionId = subscriptionId;
	}

	void initialize() throws IOException {
		subscriberStub = SubscriberGrpc.newBlockingStub(newChannel());
	}

	void acknowledge(List<String> idsToAcknowledge) throws IOException {
		subscriberStub.acknowledge(acknowledgeRequest(idsToAcknowledge));
	}

	private Channel newChannel() throws IOException {
		return NettyChannelBuilder
				.forAddress(PUBSUB_ADDRESS, PUBSUB_PORT)
				.negotiationType(NegotiationType.TLS)
				.sslContext(GrpcSslContexts.forClient().ciphers(null).build())
				.build();
	}

	private AcknowledgeRequest acknowledgeRequest(List<String> idsToAcknowledge) {
		return AcknowledgeRequest.newBuilder()
								.addAllAckIds(idsToAcknowledge)
								.setSubscription(subscriptionPath())
								.build();
	}

	private String subscriptionPath() {
		return String.format("projects/%s/subscriptions/%s", projectId, subscriptionId);
	}
}
