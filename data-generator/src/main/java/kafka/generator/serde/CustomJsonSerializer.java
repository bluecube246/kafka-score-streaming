package kafka.generator.serde;

import kafka.generator.constant.Topics;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

import com.fasterxml.jackson.databind.ObjectMapper;

public class CustomJsonSerializer implements Serializer<Object> {
    private ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public byte[] serialize(String topic, Object data) {
        try {
            if (data == null){
                System.out.println("Null received at serializing. topic: " +  topic);
                return null;
            }
            System.out.println("Serializing...");
            switch(topic) {
                case Topics.TOPIC_SCORE:
                    return objectMapper.writeValueAsBytes(data);
                default:
                    System.out.println("Unknown topic: " + topic);
            }
            return null;
        } catch (Exception e) {
            throw new SerializationException("Error when serializing data to byte[]");
        }
    }
}
