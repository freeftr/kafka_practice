package com.example;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.config.ConfigResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

public class KafkaAdminClient {

    private final static Logger logger = LoggerFactory.getLogger(KafkaAdminClient.class);
    private final static String BOOTSTRAP_SERVERS = "localhost:9092";

    public static void main(String[] args) throws Exception {

        /**
         * AdminClient.create의 반환값으로 KafkaAdminClient을 받음.
         * 브로커들의 옵션을 확인하고, 설정할 수 있는 유틸 클래스이다.
         */
        Properties configs = new Properties();
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        AdminClient admin = AdminClient.create(configs);

        /*
        브로커 정보 조회
         */
        logger.info("==Get broker information");
        for (Node node : admin.describeCluster().nodes().get()) {
            logger.info("node : {}", node);
            ConfigResource cr = new ConfigResource(
                    ConfigResource.Type.BROKER,
                    node.idString()
            );
            DescribeConfigsResult describeConfigsResult = admin.describeConfigs(Collections.singleton(cr));
            describeConfigsResult.all().get().forEach((broker, config) -> {
                config.entries().forEach(configEntry ->
                        logger.info(
                                configEntry.name() +
                                        "= " +
                                        configEntry.value()
                        ));
            });
        }

        /*
        토픽 정보 조회
         */
        logger.info("==Get default num.partitions");
        for (Node node : admin.describeCluster().nodes().get()) {
            ConfigResource cr = new ConfigResource(
                    ConfigResource.Type.BROKER,
                    node.idString()
            );
            DescribeConfigsResult describeConfigsResult = admin.describeConfigs(Collections.singleton(cr));
            Config config = describeConfigsResult.all().get().get(cr);
            Optional<ConfigEntry> optionalConfigEntry = config.entries()
                    .stream()
                    .filter(v ->
                            v.name().equals("num.partitions")).findFirst();
            ConfigEntry numPartitionConfig = optionalConfigEntry
                    .orElseThrow(Exception::new);
            logger.info("num.partitions: {}", numPartitionConfig.value());
        }

        /*
        토픽 목록 조회
         */
        logger.info("== Topic list");
        for (TopicListing topicListing : admin.listTopics().listings().get()) {
            logger.info("{}", topicListing.toString());
        }

        /*
        토픽 정보 조회
         */
        logger.info("== test topic information");
        Map<String, TopicDescription> topicInformation = admin.describeTopics(Collections.singletonList("test")).all().get();
        logger.info("{}", topicInformation);

        /*
        컨슈머 그룹 조회
         */
        logger.info("== Consumer group list");
        ListConsumerGroupsResult listConsumerGroups = admin.listConsumerGroups();
        listConsumerGroups.all().get().forEach(v -> {
            logger.info("{}", v);
        });

        admin.close();
    }
}
