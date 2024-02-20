package kafka.generator.serde;

import kafka.generator.constant.Topics;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import com.fasterxml.jackson.databind.ObjectMapper;

import kafka.generator.event.ScoreEvent;

public class CustomJsonDeserializer implements Deserializer<Object>{

    private ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public Object deserialize(String topic, byte[] data) {
        try {
            if (data == null){
                System.out.println("Null received at deserializing");
                return null;
            }
            System.out.println("Deserializing ... data of topic + " + topic );
            switch(topic) {
                case Topics.TOPIC_SCORE:
                    return objectMapper.readValue(new String(data, "UTF-8"), ScoreEvent.class);
                default:
                    System.out.println("Unknown topic: " + topic);
            }
            return null;
        } catch (Exception e) {
            if(data != null) {
                System.err.println("error occurred by data + " + new String(data));
            }
            throw new SerializationException("Error when deserializing byte[] to Class.");
        }
    }

}
