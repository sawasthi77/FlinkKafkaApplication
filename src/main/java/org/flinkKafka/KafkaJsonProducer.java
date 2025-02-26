package org.flinkKafka;

import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class KafkaJsonProducer {
    public static void main(String[] args) throws JsonProcessingException {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        ObjectMapper objectMapper = new ObjectMapper();

        for(int i = 1; i <= 9; i++){
            String key = "00000000000" + i;
            KafkaValue kafkaValue = new KafkaValue(key, "2022-01-01", "aaaaa", "DB" + i);
            String jsonValue = objectMapper.writeValueAsString(kafkaValue);

            List<RecordHeader> headers = new ArrayList<>();
            headers.add(new RecordHeader("source", "bulk-app".getBytes(StandardCharsets.UTF_8)));
            headers.add(new RecordHeader("timestamp", String.valueOf(System.currentTimeMillis()).getBytes(StandardCharsets.UTF_8)));

            ProducerRecord<String, String> record = new ProducerRecord<String, String>("input-topic", null, key, jsonValue);
            producer.send(record);
        }

        producer.close();
    }
}
