package com.example;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;

public class ConsumerAsyncCommit {
    private final static Logger logger = LoggerFactory.getLogger(ConsumerAsyncCommit.class);
    private final static String TOPIC = "topic";
    private final static String BOOTSTRAP_SERVERS = "bootstrap.servers";
    private final static String GROUP_ID = "group.id";

    public static void main(String[] args) {
        Properties configs = new Properties();
        configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        configs.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        /**
         * 명시적 오프셋 커밋 시 false
         */
        configs.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(configs);
        consumer.subscribe(Arrays.asList(TOPIC));

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
            for (ConsumerRecord<String, String> record : records) {
                logger.info("record:{}", record);
            }
            /**
             * commitAsync()의 응답을 받을 수 있도록 도와주는 콜백 인터페이스.
             */
            consumer.commitAsync(new OffsetCommitCallback() {
                public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
                    if (exception != null) {
                        System.err.println("Failed to commit offsets");
                    } else {
                        System.out.println("Offsets committed");
                    }

                    if (exception != null) {
                        logger.error("Failed to commit offsets", exception);
                    }
                }
            });
        }
    }
}
