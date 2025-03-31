package org.supreme.serdes;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.supreme.models.DepartMentWiseSalary;

import com.fasterxml.jackson.databind.ObjectMapper;

public class DepartMentWiseSalarySerde implements Serde<DepartMentWiseSalary> {
    static ObjectMapper objectMapper = new ObjectMapper();
    private final Serde<DepartMentWiseSalary> inner;
    public DepartMentWiseSalarySerde() {
        this.inner = Serdes.serdeFrom(new DepartMentWiseSalarySerializer(), new DepartMentWiseSalaryDeserializer());
    }
    @Override
    public Serializer<DepartMentWiseSalary> serializer() {
        return inner.serializer();
    }
    @Override
    public Deserializer<DepartMentWiseSalary> deserializer() {
        return inner.deserializer();
    }
    public static class DepartMentWiseSalarySerializer implements Serializer<DepartMentWiseSalary> {
        @Override
        public byte[] serialize(String topic, DepartMentWiseSalary data) {
            try{
                if (data == null) {
                    return null;
                }
                return objectMapper.writeValueAsBytes(data);
            }catch(Exception e){
                throw new IllegalArgumentException("Error serializing JSON message", e);
            }
        }
    }
    public static class DepartMentWiseSalaryDeserializer implements Deserializer<DepartMentWiseSalary> {
        @Override
        public DepartMentWiseSalary deserialize(String topic, byte[] data) {
            try{
                if (data == null) {
                    return null;
                }
                return objectMapper.readValue(data, DepartMentWiseSalary.class);
            }catch(Exception e){
                throw new IllegalArgumentException("Error deserializing JSON message", e);
            }
        }
    }
    public static DepartMentWiseSalarySerde get() {
        return new DepartMentWiseSalarySerde();
    }
}