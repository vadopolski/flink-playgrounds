
package org.apache.flink.playgrounds.ops.clickcount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.playgrounds.ops.clickcount.records.DeliveryRide;
import org.apache.flink.playgrounds.ops.clickcount.records.DeliveryRideEventDeserializationSchema;
import org.apache.flink.playgrounds.ops.clickcount.records.DeliveryRideEventSerializationSchema;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.cassandra.CassandraSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.stream.StreamSupport;

public class DeliveryEventCount {
	public static final String CHECKPOINTING_OPTION = "checkpointing";
	public static final String EVENT_TIME_OPTION = "event-time";
	public static final String OPERATOR_CHAINING_OPTION = "chaining";

	public static void main(String[] args) throws Exception {
		final ParameterTool params = ParameterTool.fromArgs(args);
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		configureEnvironment(params, env);

		String inputTopic = params.get("input-topic", "input");
		String outputTopic = params.get("output-topic", "output");
		String brokers = params.get("bootstrap.servers", "localhost:9092");
		Properties kafkaProps = new Properties();
		kafkaProps.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
		kafkaProps.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "click-event-count");

		DataStream<DeliveryRide> eventSource =
				env.addSource(new FlinkKafkaConsumer<>(inputTopic, new DeliveryRideEventDeserializationSchema(), kafkaProps))
			.name("DeliveryEvent Source");

		DataStream<Tuple2<Long, Boolean>> result = eventSource
				.flatMap(new FlatMapFunction<DeliveryRide, Tuple2<Long, Boolean>>() {
					@Override
					public void flatMap(DeliveryRide value, Collector<Tuple2<Long, Boolean>> out) {
						out.collect(new Tuple2<Long, Boolean>(value.deliveryId, value.isStart));
					}
				});

		CassandraSink.addSink(result)
				.setQuery("INSERT INTO missing.delivery(ride_id, is_start) values (?, ?)")
				.setHost("cassandra")
				.build()
				.name("DeliveryEvent Cassandra Sink");


/**
		DataStream<Tuple2<Boolean, Long>> groupedResult = eventSource
				.map(new MapFunction<DeliveryRide, Tuple2<Boolean, Long>>() {
			@Override
			public Tuple2<Boolean, Long> map(DeliveryRide deliveryRide) throws Exception {
				return new Tuple2<>(deliveryRide.isStart, 1L);
			}})
				.keyBy(value -> value.f0)
						.window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
								.sum(1);
*/

/** Not working
		DataStream<Tuple3<Long, Boolean, Long>> groupedResult = eventSource
				.keyBy((DeliveryRide fare) -> fare.isStart)
				.timeWindow(Time.seconds(5))
				.process(new GroupDelivery());


		CassandraSink.addSink(groupedResult)
				.setQuery("INSERT INTO missing.grouped_delivery(window_end, is_start, count_delivery) values (?, ?, ?)")
				.setHost("cassandra")
				.build()
				.name("DeliveryEvent Cassandra Sink");
*/

/** Kafka
		eventSource
				.addSink(new FlinkKafkaProducer<DeliveryRide>(
						outputTopic,
						new DeliveryRideEventSerializationSchema(outputTopic),
						kafkaProps,
						FlinkKafkaProducer.Semantic.AT_LEAST_ONCE))
				.name("DeliveryEvent Kafka Sink");
*/

/** HDFS
		DataStream<String> eventProcessed = eventSource
				.map(i -> i.toString())
				.name("DeliveryEvent Mapper");

		final FileSink<String> sink = FileSink
				.forRowFormat(new Path("/tmp/flink-output"), new SimpleStringEncoder<String>("UTF-8"))
				.withRollingPolicy(
						DefaultRollingPolicy.builder()
								.withRolloverInterval(TimeUnit.SECONDS.toMillis(15))
								.withInactivityInterval(TimeUnit.SECONDS.toMillis(5))
								.withMaxPartSize(128 * 128 * 128)
								.build())
				.build();

		eventProcessed.sinkTo(sink)
				.name("DeliveryEvent File Sink");
*/

		env.execute("Delivery Event Saving");
	}


	private static void configureEnvironment(
			final ParameterTool params,
			final StreamExecutionEnvironment env) {

		boolean checkpointingEnabled = params.has(CHECKPOINTING_OPTION);
		boolean eventTimeSemantics = params.has(EVENT_TIME_OPTION);
		boolean enableChaining = params.has(OPERATOR_CHAINING_OPTION);

		if (checkpointingEnabled) {
			env.enableCheckpointing(1000);
		}

		if (eventTimeSemantics) {
			env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		}

		if(!enableChaining){
			//disabling Operator chaining to make it easier to follow the Job in the WebUI
			env.disableOperatorChaining();
		}
	}


	private static class GroupDelivery extends ProcessWindowFunction<DeliveryRide, Tuple3<Long, Boolean, Long>, Boolean, TimeWindow> {
		@Override
		public void process(Boolean key, Context context, Iterable<DeliveryRide> rides, Collector<Tuple3<Long, Boolean, Long>> out) throws Exception {
			long count = StreamSupport.stream(rides.spliterator(), false).count();

			out.collect(new Tuple3<Long, Boolean, Long>(context.window().getEnd(), key, count));
		}
	}
}
