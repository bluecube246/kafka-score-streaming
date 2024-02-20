package org.example;

import java.util.Properties;

import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import kafka.generator.Utils;
import kafka.generator.constant.Topics;
import kafka.generator.event.ScoreEvent;
import kafka.generator.generator.RandomGenerator;
import kafka.generator.serde.CustomJsonSerializer;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SampleProducer {
    public static void main(String[] args) {

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "127.0.0.1:29092");
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", CustomJsonSerializer.class.getName());

        try(Producer<String, Object> producer = new KafkaProducer<>(properties)){
            while(true){
                long currentTimestamp = System.currentTimeMillis();
                ScoreEvent scoreEvent = RandomGenerator.generateScoreEvent(currentTimestamp);
                ProducerRecord<String, Object> scoreData = new ProducerRecord<>(
                        Topics.TOPIC_SCORE,
                        scoreEvent.getUserId(),
                        scoreEvent
                );

                producer.send(scoreData, (metadata, exception) -> {
                    if (exception != null){
                        log.error("Failed to produce record " + scoreEvent);
                    }else{
                        Utils.printRecordMetadata(metadata);
                    }
                });

                Thread.sleep(500);

            }
        } catch (Exception e) {
            log.error(e.getMessage());
            e.printStackTrace();
        }
    }
}