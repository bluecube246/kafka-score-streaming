package kafka.streams.serde;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

import com.fasterxml.jackson.databind.ObjectMapper;

import kafka.generator.constant.Topics;
import kafka.streams.constants.StateStores;

public class CustomJsonSerializer implements Serializer<Object> {
    private ObjectMapper objectMapper = new ObjectMapper();
    private String applicationId = StateStores.APPLICATION_ID;

    @Override
    public byte[] serialize(String topic, Object data) {
        try {
            if (data == null) {
                System.out.println("Null received at serializing. topic: " + topic);
                return null;
            }
            System.out.println("Serializing... topic: " + topic + ", data: " + data);
            switch (topic) {
                case Topics.TOPIC_SCORE:
                    return objectMapper.writeValueAsBytes(data);
                default:
                    if (!topic.startsWith(applicationId)) {
                        throw new Exception("Unknown topic: " + topic + ", data for serializing: " + data);
                    }
                    return objectMapper.writeValueAsBytes(data);
            }
        } catch (Exception e) {
            throw new SerializationException("Error when serializing data to byte[]", e);
        }
    }
}