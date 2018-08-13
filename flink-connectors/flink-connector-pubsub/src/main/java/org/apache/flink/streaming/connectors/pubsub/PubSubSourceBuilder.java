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

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.streaming.connectors.pubsub.common.SerializableCredentialsProvider;

import com.google.api.gax.core.CredentialsProvider;
import com.google.auth.Credentials;
import com.google.pubsub.v1.ProjectSubscriptionName;

import java.io.IOException;
import java.util.Optional;

/**
 * Factory class to create a SourceFunction with custom options.
 */
public class PubSubSourceBuilder<OUT> {
	private SerializableCredentialsProvider serializableCredentialsProvider;
	private DeserializationSchema<OUT>      deserializationSchema;
	private String                          projectName;
	private String                          subscriptionName;
	private String                          hostAndPort;
	private Long                            boundedByAmountOfMessages;
	private Long                            boundedByTimeSinceLastMessage;

	public static <OUT> PubSubSourceBuilder<OUT> builder() {
		return new PubSubSourceBuilder<>();
	}

	/**
	 * Set the credentials.
	 * If this is not used then the credentials are picked up from the environment variables.
	 * @param credentials the Credentials needed to connect.
	 * @return The current Builder instance
	 */
	public PubSubSourceBuilder<OUT> withCredentials(Credentials credentials) {
		this.serializableCredentialsProvider = new SerializableCredentialsProvider(credentials);
		return this;
	}

	/**
	 * Set the CredentialsProvider.
	 * If this is not used then the credentials are picked up from the environment variables.
	 * @param credentialsProvider the custom SerializableCredentialsProvider instance.
	 * @return The current Builder instance
	 */
	public PubSubSourceBuilder<OUT> withCredentialsProvider(CredentialsProvider credentialsProvider) throws IOException {
		return withCredentials(credentialsProvider.getCredentials());
	}

	/**
	 * @param deserializationSchema Instance of a DeserializationSchema that converts the OUT into a byte[]
	 * @return The current Builder instance
	 */
	public PubSubSourceBuilder<OUT> withDeserializationSchema(DeserializationSchema<OUT> deserializationSchema) {
		this.deserializationSchema = deserializationSchema;
		return this;
	}

	/**
	 * @param projectName The name of the project in GoogleCloudPlatform
	 * @param subscriptionName The name of the subscription in PubSub
	 * @return The current Builder instance
	 */
	public PubSubSourceBuilder<OUT> withProjectSubscriptionName(String projectName, String subscriptionName) {
		this.projectName = projectName;
		this.subscriptionName = subscriptionName;
		return this;
	}

	/**
	 * Set the custom hostname/port combination of PubSub.
	 * The ONLY reason to use this is during tests with the emulator provided by Google.
	 * @param hostAndPort The combination of hostname and port to connect to ("hostname:1234")
	 * @return The current instance
	 */
	public PubSubSourceBuilder<OUT> withHostAndPort(String hostAndPort) {
		this.hostAndPort = hostAndPort;
		return this;
	}

	public PubSubSourceBuilder<OUT> boundedByAmountOfMessages(long maxAmountOfMessages) {
		this.boundedByAmountOfMessages = maxAmountOfMessages;
		return this;
	}

	public PubSubSourceBuilder<OUT> boundedByTimeSinceLastMessage(long timeSinceLastMessage) {
		this.boundedByTimeSinceLastMessage = timeSinceLastMessage;
		return this;
	}

	private Optional<Bound<OUT>> getBound() {
		if (boundedByAmountOfMessages != null && boundedByTimeSinceLastMessage != null) {
			return Optional.of(Bound.boundByAmountOfMessagesOrTimeSinceLastMessage(boundedByAmountOfMessages, boundedByTimeSinceLastMessage));
		}

		if (boundedByAmountOfMessages != null) {
			return Optional.of(Bound.boundByAmountOfMessages(boundedByAmountOfMessages));
		}

		if (boundedByTimeSinceLastMessage != null) {
			return Optional.of(Bound.boundByTimeSinceLastMessage(boundedByTimeSinceLastMessage));
		}

		return Optional.empty();
	}

	/**
	 * Actually build the desired instance of the PubSubSourceBuilder.
	 * @return a brand new SourceFunction
	 * @throws IOException incase of a problem getting the credentials
	 * @throws IllegalArgumentException incase required fields were not specified.
	 */
	public PubSubSource<OUT> build() throws IOException {
		if (serializableCredentialsProvider == null) {
			serializableCredentialsProvider = SerializableCredentialsProvider.credentialsProviderFromEnvironmentVariables();
		}
		if (deserializationSchema == null) {
			throw new IllegalArgumentException("The deserializationSchema has not been specified.");
		}
		if (projectName == null || subscriptionName == null) {
			throw new IllegalArgumentException("The ProjectName And SubscriptionName has not been specified.");
		}

		SubscriberWrapper subscriberWrapper =
			new SubscriberWrapper(serializableCredentialsProvider, ProjectSubscriptionName.of(projectName, subscriptionName));

		if (hostAndPort != null) {
			subscriberWrapper.withHostAndPort(hostAndPort);
		}

		Optional<Bound<OUT>> bound = getBound();
		if (bound.isPresent()) {
			return new BoundedPubSubSource<>(subscriberWrapper, deserializationSchema, bound.get());
		}

		return new PubSubSource<>(subscriberWrapper, deserializationSchema);
	}
}
