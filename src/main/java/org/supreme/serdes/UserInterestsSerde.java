package org.supreme.serdes;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.supreme.models.UserInterests;
import com.fasterxml.jackson.databind.ObjectMapper;

public class UserInterestsSerde implements Serde<UserInterests>{

    static ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public UserInterestsDeserializer deserializer() {
        return new UserInterestsDeserializer();
    }

    @Override
    public UserInterestsSerializer serializer() {
        return new UserInterestsSerializer();
    }

    public static class UserInterestsSerializer implements Serializer<UserInterests> {

        @Override
        public byte[] serialize(String topic, UserInterests data) {
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

    public static class UserInterestsDeserializer implements Deserializer<UserInterests> {

        @Override
        public UserInterests deserialize(String topic, byte[] data) {
            if (data == null) {
                return null;
            }
            try {
                return objectMapper.readValue(data, UserInterests.class);
            } catch (Exception e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
                return null;
            }
        }
    }

    public static UserInterestsSerde get() {
        return new UserInterestsSerde();
    }
    
}
