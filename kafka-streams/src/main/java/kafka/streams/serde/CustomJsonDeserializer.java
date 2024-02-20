package kafka.streams.serde;


import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import com.fasterxml.jackson.databind.ObjectMapper;

import kafka.streams.constants.StateStores;
import kafka.generator.constant.Topics;
import kafka.generator.event.ScoreEvent;

public class CustomJsonDeserializer implements Deserializer<Object> {
    private ObjectMapper objectMapper = new ObjectMapper();

    private String separator = "-";
    private String applicaitonId = StateStores.APPLICATION_ID;

    @Override
    public Object deserialize(String topic, byte[] data) {
        try {
            if (data == null) {
                System.out.println("Null received at deserializing");
                return null;
            }
            System.out.println("Deserializing ... data of topic + " + topic);
            switch (topic) {
                case Topics.TOPIC_SCORE:
                    return objectMapper.readValue(new String(data, "UTF-8"), ScoreEvent.class);
                default:
                    if (topic.startsWith(applicaitonId + separator + StateStores.CLICK_COUNT_BY_AD)) {
                        return objectMapper.readValue(new String(data, "UTF-8"), ScoreEvent.class);
                    }

                    throw new Exception("Unknown topic: " + topic + ", data: " + new String(data));
            }
        } catch (Exception e) {
            if (data != null) {
                System.err.println("error occurred by data + " + new String(data));
            }
            throw new SerializationException("Error when deserializing byte[] to Class.", e);
        }
    }
}