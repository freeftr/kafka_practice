package com.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class SimpleProducer {

    // 전송하고자 하는 토픽
    private final static Logger logger = LoggerFactory.getLogger(SimpleProducer.class);
    private final static String TOPIC_NAME = "test";
    private final static String BOOSTRAP_SERVERS = "kafka:9092";

    public static void main(String[] args) {

        /**
         KafkaProducer 인스턴스를 생성하기 위한 프로듀서 옵션들을 key/value 값으로 선언.
         필수 옵션은 반드시 선언, 선택 옵션은 선택 안할시 기본 값으로
         => 선택 옵션에는 뭐가 있는지, 옵션값이 뭔지는 알아야 한다.
         **/
        Properties configs = new Properties();
        configs.put(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                BOOSTRAP_SERVERS
        ); // 카프카 클러스터 서버 host, IP
        configs.put(
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName()
        ); // 메시지 직렬화
        configs.put(
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName()
        );

        // KafkaProducer 인스턴스 생성
        KafkaProducer<String, String> producer = new KafkaProducer<>(configs);

        String message = "Hello World!";
        // 생성자 여러개, 오버로딩 가능
        // 생성자에 들어가는 2개의 제너릭 값은 메시지 키, 메시지 값
        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, message);
        // 바로 보내는 것이 아니다... 프로듀서 내부에 가지고 있다가 배치로 처리
        producer.send(record);
        logger.info("record: {}", record);
        // 브로커로 전송
        producer.flush();
        producer.close();
    }
}
