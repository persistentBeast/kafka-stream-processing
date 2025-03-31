package org.supreme.serdes;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.supreme.models.AdDetails;

import com.fasterxml.jackson.databind.ObjectMapper;

public class AdDetailsSerde implements Serde<AdDetails>{

        static ObjectMapper objectMapper = new ObjectMapper();
        private final Serde<AdDetails> inner;

        public AdDetailsSerde() {
            this.inner = Serdes.serdeFrom(new AdDetailsSerializer(), new AdDetailsDeserializer());
        }

        @Override
        public Serializer<AdDetails> serializer() {
            return inner.serializer();
        }

        @Override
        public Deserializer<AdDetails> deserializer() {
            return inner.deserializer();
        }

        public static class AdDetailsSerializer implements Serializer<AdDetails> {

            @Override
            public byte[] serialize(String topic, AdDetails data) {
                if (data == null) {
                    return null;
                }
                try {
                    return objectMapper.writeValueAsBytes(data);
                } catch (Exception e) {
                    throw new IllegalArgumentException("Error serializing JSON message", e);
                }
            }
        }

        public static class AdDetailsDeserializer implements Deserializer<AdDetails> {

            @Override
            public AdDetails deserialize(String topic, byte[] data) {
                try {
                    if (data == null) {
                        return null;
                    }
                    return objectMapper.readValue(data, AdDetails.class);
                } catch (Exception e) {
                    throw new IllegalArgumentException("Error deserializing JSON message", e);
                }
            }
        }

        public static AdDetailsSerde get() {
            return new AdDetailsSerde();
        }
    
}
