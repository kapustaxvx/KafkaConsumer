package com.example.kafkaconsumerex.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.PartitionOffset;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import static com.example.kafkaconsumerex.config.KafkaConsumerConfig.TOPIC_1;
import static com.example.kafkaconsumerex.config.KafkaConsumerConfig.TOPIC_2;

@Service
public class KafkaConsumerService {
    private final Logger LOG = LoggerFactory.getLogger(KafkaConsumerService.class);

    @KafkaListener(topics = {TOPIC_1, TOPIC_2}, groupId = "${kafka.groupId}", topicPartitions = {
            @TopicPartition(topic = TOPIC_1, partitions = "0"),
            @TopicPartition(topic = TOPIC_2, partitions = "0-2")
    })
    public void listenGroupFoo(@Payload String message,
                               @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition,
                               @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
                               @Header(KafkaHeaders.RECEIVED_TIMESTAMP) long ts,
                               @Header(KafkaHeaders.GROUP_ID) String groupId
    ) {
        LOG.info("Received Message[{}], in group: {}, from topic:{}  partition:{} timestamp: {}", message, groupId, topic, partition, ts);
    }

    @KafkaListener(topics = TOPIC_2, groupId = "another group", topicPartitions = {
            @TopicPartition(topic = TOPIC_2
                    , partitionOffsets = @PartitionOffset(partition = "3-5", initialOffset = "0"))
    })
    public void listen(@Payload String message,
                       @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition,
                       @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
                       @Header(KafkaHeaders.RECEIVED_TIMESTAMP) long ts,
                       @Header(KafkaHeaders.GROUP_ID) String groupId
    ) {
        LOG.info("Received Message[{}], in group: {}, from topic:{}  partition:{} timestamp: {}", message, groupId, topic, partition, ts);
    }
}
