package com.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerWithKeyValue {
    private final static String TOPIC_NAME = "test";
    private final static String BOOTSTRAP_SERVERS = "kafka:9092";

    public static void main(String[] args) {

        Properties configs = new Properties();
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(configs);

        /**
         * 메시지에 키를 추가하려면 ProducerRecord에 (토픽이름, Key, Value)로 보내면 된다.
         */
        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, "key", "value");
        producer.send(record);
        ProducerRecord<String, String> record2 = new ProducerRecord<>(TOPIC_NAME, "key2", "value2");
        producer.send(record2);
        producer.flush();
        producer.close();
    }
}
