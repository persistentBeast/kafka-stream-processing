package org.supreme.kStreams;

import java.security.Timestamp;
import java.sql.Time;
import java.time.Duration;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.springframework.stereotype.Component;
import org.supreme.models.AdDetails;
import org.supreme.serdes.AdDetailsSerde;
import jakarta.annotation.PostConstruct;




//k - minutes Add click CTR. 
@Component
public class AddClickCTR {


    @PostConstruct
    public void startStream(){

        StreamsBuilder builder = new StreamsBuilder();

        Properties props = new Properties();
        props.put("application.id", "kafka-streams-ad-click-ctr-app-2");
        props.put("bootstrap.servers", "localhost:9092");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams-ad-click-ctr-app");

        String addimpressionsTopic = "ad-impressions-topic";
        String adClicksTopic = "ad-clicks-topic";

        KStream<String, AdDetails> adImpressionsStream = builder.stream(addimpressionsTopic,
         Consumed.with(Serdes.String(), AdDetailsSerde.get()).withTimestampExtractor((record, previousTimestamp) -> {
            return ((AdDetails)record.value()).getTimestamp();
        }));

        KTable<Windowed<String>, Long> adImpressionsTable = adImpressionsStream.groupBy((k, v) -> v.getCampainer(), 
            Grouped.with(Serdes.String(), AdDetailsSerde.get()))
        .windowedBy(TimeWindows.ofSizeAndGrace(Duration.ofMinutes(2), Duration.ofSeconds(0)))
        .count();

        // adImpressionsTable.toStream().foreach((k, v) -> {
        //     System.out.println("Key: " + k + " Value: " + v);
        // });

        KStream<String, AdDetails> adClicksStream = builder.stream(adClicksTopic,
         Consumed.with(Serdes.String(), AdDetailsSerde.get()).withTimestampExtractor((record, previousTimestamp) -> {
            return ((AdDetails)record.value()).getTimestamp();
        }));

        KTable<Windowed<String>, Long> adClicksTable = adClicksStream.groupBy((k, v) -> v.getCampainer(), 
            Grouped.with(Serdes.String(), AdDetailsSerde.get()))
        .windowedBy(TimeWindows.ofSizeAndGrace(Duration.ofMinutes(2), Duration.ofSeconds(0)))
        .count();

        // adClicksTable.toStream().foreach((k, v) -> {
        //     System.out.println("Ad Clicks Key: " + k + " Value: " + v);
        // });

        KTable<Windowed<String>, Double> ctrTable = adClicksTable.join(adImpressionsTable, 
            (clicks, impressions ) -> {
                if(clicks == 0){
                    return 0.0;
                }
                return (double)clicks / impressions;
            });

        ctrTable.toStream().foreach((k, v) -> {
            System.out.println("Key: " + k + " Value: " + v);
        });

        KafkaStreams ks = new KafkaStreams(builder.build(), props);
        ks.start();

        Runtime.getRuntime().addShutdownHook(new Thread(ks::close));

    }
    
}
