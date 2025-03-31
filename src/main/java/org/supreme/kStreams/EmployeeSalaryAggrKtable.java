// package org.supreme.kStreams;

// import java.util.ArrayList;
// import java.util.List;
// import java.util.Properties;

// import org.apache.kafka.clients.consumer.ConsumerConfig;
// import org.apache.kafka.common.serialization.Serde;
// import org.apache.kafka.common.serialization.Serdes;
// import org.apache.kafka.streams.KafkaStreams;
// import org.apache.kafka.streams.KeyValue;
// import org.apache.kafka.streams.StreamsBuilder;
// import org.apache.kafka.streams.kstream.Consumed;
// import org.apache.kafka.streams.kstream.Grouped;
// import org.apache.kafka.streams.kstream.KGroupedStream;
// import org.apache.kafka.streams.kstream.KGroupedTable;
// import org.apache.kafka.streams.kstream.KStream;
// import org.apache.kafka.streams.kstream.KTable;
// import org.apache.kafka.streams.kstream.Materialized;
// import org.springframework.stereotype.Component;
// import org.supreme.models.DepartMentWiseSalary;
// import org.supreme.models.EmployeeSalary;
// import org.supreme.serdes.DepartMentWiseSalarySerde;
// import org.supreme.serdes.EmployeeSalarySerde;

// import jakarta.annotation.PostConstruct;

// @Component
// public class EmployeeSalaryAggrKtable {

//     @PostConstruct
//     public void startStream(){

//         StreamsBuilder builder = new StreamsBuilder();

//         Properties props = new Properties();
//         props.put("application.id", "kafka-streams-test-app-stream-5");
//         props.put("bootstrap.servers", "localhost:9092");
//         props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
    
//         String topic = "employee-salary-topic";

//         KTable<String, EmployeeSalary> kTable = builder
//         .table(topic, Consumed.with(Serdes.String(), EmployeeSalarySerde.get()));

//         KGroupedTable<String, List<EmployeeSalary>> kGroupedTable = kTable
//         .groupBy((k, v) -> new KeyValue(v.getDepartment(), List.of(v)), 
//         Grouped.with(Serdes.String(), Serdes.ListSerde(ArrayList.class, EmployeeSalarySerde.get())));

//         KTable<String, DepartMentWiseSalary> kTableAggr = kGroupedTable.aggregate(
//             () -> new DepartMentWiseSalary()
//         );

//         KafkaStreams ks = new KafkaStreams(builder.build(), props);
//         ks.start();

//         Runtime.getRuntime().addShutdownHook(new Thread(ks::close));
        
//     }
    
// }
