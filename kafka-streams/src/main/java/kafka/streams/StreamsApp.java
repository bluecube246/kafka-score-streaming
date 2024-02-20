package kafka.streams;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.Topology.AutoOffsetReset;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.Stores;

import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.server.Server;
import com.linecorp.armeria.server.ServerBuilder;
import com.linecorp.armeria.server.annotation.Get;
import com.linecorp.armeria.server.annotation.Param;

import kafka.generator.constant.Topics;
import kafka.generator.event.ScoreEvent;
import kafka.streams.constants.StateStores;
import kafka.streams.extractor.CustomTimestampExtractor;
import kafka.streams.serde.CustomJsonSerde;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class StreamsApp {

    public static void main(String[] args) {
        KafkaStreams kafkaStreams = startStreams("127.0.0.1:29092");
        kafkaStreams.start();

        ServerBuilder sb = Server.builder();
        // Configure an HTTP port.
        sb.http(8080);
        Server server = sb
                .annotatedService(new Object() {
                    @Get("/count")
                    public HttpResponse count(@Param("adId") String adId) {

                        String storeName = StateStores.CLICK_COUNT_BY_AD;
                        ReadOnlyKeyValueStore<String, Integer> countByAd = kafkaStreams.store(
                                StoreQueryParameters.fromNameAndType(storeName,
                                        QueryableStoreTypes.keyValueStore()));

                        Integer count = countByAd.get(adId);
                        if (count == null) {
                            HttpResponse.of(HttpStatus.UNPROCESSABLE_ENTITY);
                        }
                        return HttpResponse.of(String.format("{ adId: %s, count: %d }", adId, count));
                    }
                })
                .build();
        CompletableFuture<Void> future = server.start();
        // Wait until the server is ready.
        future.join();
    }

    public static KafkaStreams startStreams(String bootstrapServers) {
        final Duration window = Duration.ofMinutes(1);

        final StreamsBuilder streamsBuilder = new StreamsBuilder();
        final Consumed<String, Object> consumed = Consumed.with(Serdes.String(), new CustomJsonSerde())
                .withTimestampExtractor(new CustomTimestampExtractor())
                .withOffsetResetPolicy(AutoOffsetReset.LATEST);

//        final KStream<String, ScoreEvent> impressionEventKStream =
//                streamsBuilder.stream(Topics.TOPIC_SCORE, consumed)
//                        .mapValues(
//                                value -> (ScoreEvent) value) // new ValueMapper<Object, ImpressionEvent>() {}
//                        .filter((key, value) -> isValidEventTime(System.currentTimeMillis(),
//                                value.getTimestamp()))
//                        .mapValues((key, value) -> {
//                                    log.info("valid impression event  - time diff: {}, data: {}",
//                                            Duration.ofMillis(System.currentTimeMillis() - value.getTimestamp()),
//                                            value);
//                                    return value;
//                                }
//                        );

        final KStream<String, ScoreEvent> impressionEventKStream =
                streamsBuilder.stream(Topics.TOPIC_SCORE, consumed)
                        .mapValues(
                                value -> (ScoreEvent) value) // new ValueMapper<Object, ImpressionEvent>() {}
                        .filter((key, value) -> isValidEventTime(System.currentTimeMillis(),
                                value.getTimestamp()))
                        .mapValues((key, value) -> {
                                    log.info("valid impression event  - time diff: {}, data: {}",
                                            Duration.ofMillis(System.currentTimeMillis() - value.getTimestamp()),
                                            value);
                                    return value;
                                }
                        );

        final KeyValueBytesStoreSupplier keyValueBytesStoreSupplier = Stores.inMemoryKeyValueStore(
                StateStores.CLICK_COUNT_BY_AD);

        final Materialized<String, Integer, KeyValueStore<Bytes, byte[]>>
                stateStore = Materialized.<String, Integer>as(keyValueBytesStoreSupplier)
                .withKeySerde(Serdes.String())
                .withValueSerde(Serdes.Integer())
                .withCachingEnabled();

//        impressionEventKStream.groupByKey()
//                .count(stateStore); //결과를 stateStore 이용해줘

        impressionEventKStream
                .map((key, value) -> new KeyValue<>((String) value.getUserId(), value.getScore()))
                .groupByKey(Grouped.with(Serdes.String(), Serdes.Integer()))
                .reduce(Integer::sum, stateStore);

        System.out.println(impressionEventKStream.groupByKey().toString());

        final Properties properties = new Properties();
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, StateStores.APPLICATION_ID);
        properties.setProperty(StreamsConfig.REPLICATION_FACTOR_CONFIG, "-1");
        properties.setProperty(StreamsConfig.APPLICATION_SERVER_CONFIG, "localhost:8081");
        properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, CustomJsonSerde.class.getName());
        properties.setProperty(StreamsConfig.NUM_STREAM_THREADS_CONFIG, "3");

        final Topology topology = streamsBuilder.build();
        log.info("======= Topology =======\n{}", topology.describe());
        return new KafkaStreams(topology, properties);
    }

    private static boolean isValidEventTime(long currentTimestampMills, long eventTimestampMills) {
        return !(currentTimestampMills < eventTimestampMills
                || (currentTimestampMills - eventTimestampMills) > Duration.ofMinutes(2).toMillis());
    }
}