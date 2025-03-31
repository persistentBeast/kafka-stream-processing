package org.supreme;

import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class KafkaConfig {

    // @Bean
    // StreamsBuilder streamsBuilder() {
    //     return new StreamsBuilder();
    // }

    // @Bean     
    // KafkaStreams kafkaStreams(StreamsBuilder builder) {
    //    Properties props = new Properties();
    //    props.put("application.id", "kafka-streams-test-app");
    //      props.put("bootstrap.servers", "localhost:9092");
    //      props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
    //    KafkaStreams ks = new KafkaStreams(builder.build(), props);
    //     ks.start();
    //    return ks;
    // }
    
}
