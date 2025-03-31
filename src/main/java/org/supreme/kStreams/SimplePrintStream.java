package org.supreme.kStreams;

import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.stereotype.Component;

import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
public class SimplePrintStream {

    @PostConstruct
    public void printStream() {

        StreamsBuilder builder = new StreamsBuilder();

        Properties props = new Properties();
        props.put("application.id", "kafka-streams-test-app");
        props.put("bootstrap.servers", "localhost:9092");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, "org.apache.kafka.common.serialization.Serdes$StringSerde");
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, "org.apache.kafka.common.serialization.Serdes$StringSerde");

        String topic = "input-topic";

        KStream<String, String> kStream = builder.stream(topic);

        kStream.foreach((k, v) -> {
            log.info("Key: " + k + " Value: " + v);
        });

        KafkaStreams ks = new KafkaStreams(builder.build(), props);
        ks.start();

        Runtime.getRuntime().addShutdownHook(new Thread(ks::close));
    }
    
    
}
