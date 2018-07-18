package org.apache.flink.streaming.examples.pubsub;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;
import java.math.BigInteger;

class IntegerSerializer implements DeserializationSchema<Integer>, SerializationSchema<Integer> {

	@Override
	public Integer deserialize(byte[] bytes) throws IOException {
		return new BigInteger(bytes).intValue();
	}

	@Override
	public boolean isEndOfStream(Integer integer) {
		return false;
	}

	@Override
	public TypeInformation<Integer> getProducedType() {
		return TypeInformation.of(Integer.class);
	}

	@Override
	public byte[] serialize(Integer integer) {
		return BigInteger.valueOf(integer).toByteArray();
	}
}
