package org.supreme.kStreams;

import java.time.Duration;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Suppressed;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.springframework.stereotype.Component;
import org.springframework.util.ObjectUtils;
import org.supreme.models.UserInterests;
import org.supreme.models.UserInterestsAggr;
import org.supreme.serdes.UserInterestsAggrSerde;
import org.supreme.serdes.UserInterestsSerde;

import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
public class UserInterestsCatcherStream {
    
    @PostConstruct
    public void startStream() {
        
        StreamsBuilder builder = new StreamsBuilder();

        Properties props = new Properties();
        props.put("application.id", "kafka-streams-user-interests-catcher-app-4");
        props.put("bootstrap.servers", "localhost:9092");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams-user-interests-catcher-app");
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 5);

        String userInterests = "user-interests-topic-3";


        String userInterestOutputAggrTopic = "user-interests-aggr-topic-3";

        KStream<String, UserInterests> kStream = builder.stream(userInterests, Consumed.with(Serdes.String(), UserInterestsSerde.get()));

        KTable<Windowed<String>, UserInterestsAggr> kt = kStream.groupBy((k, v) -> v.getUserId(), Grouped.with(Serdes.String(), UserInterestsSerde.get()))
        .windowedBy(TimeWindows.ofSizeAndGrace(Duration.ofMinutes(1), Duration.ofSeconds(0)))
        .aggregate(
            () -> new UserInterestsAggr(),
            (k, v, agg) -> {
                Set<String> interests = agg.getInterests();
                if(ObjectUtils.isEmpty(interests)){
                    interests = new HashSet<>();
                }
                interests.add(v.getInterest());
                agg.setUserId(k);
                agg.setInterests(interests);
                return agg;
            },
            Materialized.with(Serdes.String(), UserInterestsAggrSerde.get())
        ).suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()));

        kt.mapValues((k, v) -> {v.setWe(k.window().end()); v.setWs(k.window().start()); return v;})
        .toStream((k, v) -> k.key()).to(userInterestOutputAggrTopic, Produced.with(Serdes.String(), UserInterestsAggrSerde.get()));

        KafkaStreams ks = new KafkaStreams(builder.build(), props);
        ks.start();

        Runtime.getRuntime().addShutdownHook(new Thread(ks::close));

    }

}
