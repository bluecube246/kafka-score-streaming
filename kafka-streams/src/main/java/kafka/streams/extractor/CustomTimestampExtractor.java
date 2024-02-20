package kafka.streams.extractor;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

import kafka.generator.constant.Topics;
import kafka.generator.event.ScoreEvent;

public class CustomTimestampExtractor implements TimestampExtractor {
    @Override
    public long extract(ConsumerRecord<Object, Object> record, long partitionTime) {
        final String topic = record.topic();
        switch (topic) {
            case Topics.TOPIC_SCORE:
                return ((ScoreEvent) record.value()).getTimestamp();
            default:
                return partitionTime;
        }
    }
}
