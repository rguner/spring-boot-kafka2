package com.guner.kafka.service;

import com.guner.kafka.model.Greeting;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class KafkaReceiver {

    @KafkaListener(topics = "topic-1", groupId = "group-1")
    public void listenGroupGroup1(String message) {
        log.info("-----   Received Message in group group-1 {}", message);
    }

    @KafkaListener(topics = "topic-1", groupId = "group-2")
    public void listenGroupGroup2(String message) {
        log.info("-----   Received Message in group group-2: {}", message);
        //throw new RuntimeException("Receive Exception to test retry mechanism");
    }

    //@KafkaListener(topics = "topic-1, topic-2", groupId = "group-1")
    @KafkaListener(topics = "topic-2", groupId = "group-1")
    public void listenMultipleTopicsGroupGroup1(String message) {
        log.info("-----   Received Message on topic-2 in group group-1: {}", message);
    }


    @KafkaListener(topics = "topic-greeting", containerFactory = "jsonKafkaListenerContainerFactory")
    public void jsonObjectListener(Object object) {
        Greeting greeting = (Greeting) ((ConsumerRecord)object).value();
        log.info("-----   Received Object Message on topic-2 in group group-1: {}", object);
        log.info("-----   Received Greeting Message on topic-2 in group group-1: {}", greeting);

    }

}
