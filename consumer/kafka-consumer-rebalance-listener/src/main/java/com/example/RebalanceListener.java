package com.example;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

public class RebalanceListener implements ConsumerRebalanceListener {
    private final static Logger logger = LoggerFactory.getLogger(RebalanceListener.class);

    /**
     * 리밸런스 시작 전
     */
    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> collection) {
        logger.warn("onPartitionsRevoked: {}", collection);
    }

    /**
     * 리밸런싱 끝나고 파티션 할당 후
     */
    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> collection) {
        logger.warn("onPartitionsAssigned: {}", collection);
    }
}
