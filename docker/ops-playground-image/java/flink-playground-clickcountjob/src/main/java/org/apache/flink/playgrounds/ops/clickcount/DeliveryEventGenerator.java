package org.apache.flink.playgrounds.ops.clickcount;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.playgrounds.ops.clickcount.records.DeliveryRide;
import org.apache.flink.playgrounds.ops.clickcount.records.DeliveryRideEventSerializationSchema;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import java.util.*;


public class DeliveryEventGenerator {
	public static final long DELAY = 15;
	public static transient Long rideId = 0L;


	public static void main(String[] args) throws Exception {
		final ParameterTool params = ParameterTool.fromArgs(args);
		String topic = params.get("topic", "input");
		Properties kafkaProps = createKafkaProperties(params);
		KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(kafkaProps);

		DeliveryIterator deliveryIterator = new DeliveryIterator();

		while (true) {
			rideId++;
			ProducerRecord<byte[], byte[]> record = new DeliveryRideEventSerializationSchema(topic).serialize(deliveryIterator.next(), null);

			producer.send(record);

			Thread.sleep(DELAY);
		}
	}

	private static Properties createKafkaProperties(final ParameterTool params) {
		String brokers = params.get("bootstrap.servers", "localhost:9092");
		Properties kafkaProps = new Properties();
		kafkaProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
		kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getCanonicalName());
		kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getCanonicalName());
		return kafkaProps;
	}


	static class DeliveryIterator {
		private Map<String, Long> nextTimestampPerKey;
		private Random randomno = new Random();

		DeliveryIterator() {
			nextTimestampPerKey = new HashMap<>();
		}

		DeliveryRide next() {
			return new DeliveryRide(randomno.nextBoolean(), nextTimestamp("begin"), nextTimestamp("end"), rideId);
		}

		private Date nextTimestamp(String page) {
			long nextTimestamp = nextTimestampPerKey.getOrDefault(page, 0L);
			nextTimestampPerKey.put(page, nextTimestamp);
			return new Date(nextTimestamp);
		}
	}
}
