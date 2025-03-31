package org.supreme.serdes;

import java.io.IOException;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.supreme.models.EmployeeSalary;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.exc.StreamReadException;
import com.fasterxml.jackson.databind.DatabindException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class EmployeeSalarySerde implements Serde<EmployeeSalary> {

    static ObjectMapper objectMapper = new ObjectMapper();

    private final Serde<EmployeeSalary> inner;

    public EmployeeSalarySerde() {
        this.inner = Serdes.serdeFrom(new EmployeeSalarySerializer(), new EmployeeSalaryDeserializer());
    }

    @Override
    public Serializer<EmployeeSalary> serializer() {
        return inner.serializer();
    }

    @Override
    public Deserializer<EmployeeSalary> deserializer() {
        return inner.deserializer();
    }

    public static class EmployeeSalarySerializer implements Serializer<EmployeeSalary> {

        @Override
        public byte[] serialize(String topic, EmployeeSalary data) {
            if (data == null) {
                return null;
            }
            try {
                return objectMapper.writeValueAsBytes(data);
            } catch (JsonProcessingException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
                return null;
            }
        }
    }

    public static class EmployeeSalaryDeserializer implements Deserializer<EmployeeSalary> {

        @Override
        public EmployeeSalary deserialize(String topic, byte[] data) {
            if (data == null) {
                return null;
            }
            try {
                return objectMapper.readValue(data, EmployeeSalary.class);
            } catch (StreamReadException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            } catch (DatabindException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            return null;
        }
    }

    public static EmployeeSalarySerde get() {
        return new EmployeeSalarySerde();
    }
}