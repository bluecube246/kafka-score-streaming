package kafka.streams.serde;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

public class CustomJsonSerde implements Serde<Object> {

    @Override
    public Serializer<Object> serializer() {
        return new CustomJsonSerializer();
    }

    @Override
    public Deserializer<Object> deserializer() {
        return new CustomJsonDeserializer();
    }
}