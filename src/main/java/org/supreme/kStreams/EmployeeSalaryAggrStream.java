package org.supreme.kStreams;

import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.springframework.stereotype.Component;
import org.supreme.models.EmployeeSalary;
import org.supreme.serdes.DepartMentWiseSalarySerde;
import org.supreme.serdes.EmployeeSalarySerde;
import org.supreme.models.DepartMentWiseSalary;

import jakarta.annotation.PostConstruct;

@Component
public class EmployeeSalaryAggrStream {
    

    @PostConstruct
    public void startStream(){

        StreamsBuilder builder = new StreamsBuilder();

        Properties props = new Properties();
        props.put("application.id", "kafka-streams-test-app-stream-4");
        props.put("bootstrap.servers", "localhost:9092");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
    
        String topic = "employee-salary-topic";

        KStream<String, EmployeeSalary> kStream = builder.stream(topic, Consumed.with(Serdes.String(), EmployeeSalarySerde.get()));

        KGroupedStream<String, EmployeeSalary> kGroupedStream = kStream
        .groupBy((k, v) -> v.getDepartment(), 
        Grouped.with(Serdes.String(), EmployeeSalarySerde.get()));

        KTable<String, DepartMentWiseSalary> kTable = kGroupedStream.aggregate(
            () -> new DepartMentWiseSalary(),
            (k, v, agg) -> {
                agg.setDepartment(k);
                agg.setTotalSalary(agg.getTotalSalary() + v.getSalary());
                agg.setTotalEmployee(agg.getTotalEmployee() + 1);
                agg.setAvgSalary(agg.getTotalSalary() / agg.getTotalEmployee());
                return agg;
            },
            Materialized.with(Serdes.String(), DepartMentWiseSalarySerde.get())
        );

        kTable.toStream().foreach((k, v) -> {
            System.out.println("Key: " + k + " Value: " + v);
        });

        KafkaStreams ks = new KafkaStreams(builder.build(), props);
        ks.start();

        Runtime.getRuntime().addShutdownHook(new Thread(ks::close));
        
    }


}
