package org.supreme.serdes;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.supreme.models.UserInterestsAggr;

import com.fasterxml.jackson.databind.ObjectMapper;

public class UserInterestsAggrSerde implements Serde<UserInterestsAggr> {

    static ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public UserInterestsAggrDeserializer deserializer() {
        return new UserInterestsAggrDeserializer();
    }

    @Override
    public UserInterestsAggrSerializer serializer() {
        return new UserInterestsAggrSerializer();
    }

    public static class UserInterestsAggrSerializer implements Serializer<UserInterestsAggr> {

        @Override
        public byte[] serialize(String topic, UserInterestsAggr data) {
            if (data == null) {
                return null;
            }
            try {
                return objectMapper.writeValueAsBytes(data);
            } catch (Exception e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
                return null;
            }
        }
    }

    public static class UserInterestsAggrDeserializer implements Deserializer<UserInterestsAggr> {

        @Override
        public UserInterestsAggr deserialize(String topic, byte[] data) {
            if (data == null) {
                return null;
            }
            try {
                return objectMapper.readValue(data, UserInterestsAggr.class);
            } catch (Exception e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
                return null;
            }
        }
    }

    public static UserInterestsAggrSerde get() {
        return new UserInterestsAggrSerde();
    }


    
}
