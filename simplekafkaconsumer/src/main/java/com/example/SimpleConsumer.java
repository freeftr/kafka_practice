package com.example;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class SimpleConsumer {

    private final static Logger logger = LoggerFactory.getLogger(SimpleConsumer.class);
    private final static String TOPIC = "test";
    private final static String BOOTSTRAP_SERVERS = "kafka:9092";
    private final static String GROUP_ID = "test-group";

    public static void main(String[] args) {
        Properties configs = new Properties();
        configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        /**
         컨슈머 그룹 이름을 선언한다. => 컨슈머의 목적을 구분
         컨슈머 그룹을 기준으로 컨슈머 오프셋을 관리
         => subscribe메서드를 사용하여 토픽을 구독하는 경우에는 컨슈머 그룹을 선언해야 한다.
         **/
        configs.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        /**
         * KafkaConsumer 인스턴스 생성
         */
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(configs);
        /**
         * 컨슈머에게 토픽 할당
         */
        consumer.subscribe(Arrays.asList(TOPIC));

        /**
         * poll 메서드로 데이터를 가져오기
         */
        while (true) {
            /**
             * Durataion 타입: 브로커로부터 데이터를 가져올 때 컨슈머
             */
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));

            for (ConsumerRecord<String, String> record : records) {
                logger.info("{}", record);
            }
        }
    }
}
