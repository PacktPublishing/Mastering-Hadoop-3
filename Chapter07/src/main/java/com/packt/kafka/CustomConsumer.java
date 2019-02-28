package com.packt.kafka;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.util.*;

public class CustomConsumer {
    public static void main(String[] args) throws Exception {
        String topic = "test1";
        List<String> topicList = new ArrayList<>();
        topicList.add(topic);
        Properties consumerProperties = new Properties();
        consumerProperties.put("bootstrap.servers", "10.200.99.197:6667");
        consumerProperties.put("group.id", "Demo_Group");
        consumerProperties.put("key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProperties.put("value.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProperties.put("enable.auto.commit", "true");
        consumerProperties.put("auto.commit.interval.ms", "1000");
        consumerProperties.put("session.timeout.ms", "30000");
        KafkaConsumer<String, String> customKafkaConsumer = new
                KafkaConsumer<String, String>(consumerProperties);
        customKafkaConsumer.subscribe(topicList);


        int i = 0;
        try {
            while (true) {
                ConsumerRecords<String, String> records =
                        customKafkaConsumer.poll(500);
                for (ConsumerRecord<String, String> record : records)
                    //TODO : Do processing for data here
                    customKafkaConsumer.commitAsync(new
                                                            OffsetCommitCallback() {
                                                                public void onComplete(Map<TopicPartition,
                                                                        OffsetAndMetadata> map, Exception e) {
                                                                }
                                                            });
            }
        } catch (Exception ex) {
            //TODO : Log Exception Here
        } finally {
            try {
                customKafkaConsumer.commitSync();
            } finally {
                customKafkaConsumer.close();
            }
        }
    }
}