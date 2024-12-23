package com.guner.kafka.service;

import com.guner.kafka.config.KafkaTopicConfig;
import com.guner.kafka.model.Greeting;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.concurrent.CompletableFuture;

@Service
@RequiredArgsConstructor
public class KafkaSender {

    private final KafkaTemplate<String, String> kafkaTemplate;

    private final KafkaTemplate<String, Object> jsonKafkaTemplate;

    public void sendMessage(String msg) {
        kafkaTemplate.send(KafkaTopicConfig.TOPIC_NAME, msg);
    }

    public void sendMessage2(String msg) {
        kafkaTemplate.send(KafkaTopicConfig.TOPIC_NAME_2, msg);
    }

    public void sendMessage3(String msg) {
        kafkaTemplate.send(KafkaTopicConfig.TOPIC_NAME_3, msg);
    }

    public void sendGreetingMessage(String msg) {
        Greeting greeting = new Greeting();
        greeting.setMsg(msg);
        greeting.setName("ramazan");
        jsonKafkaTemplate.send(KafkaTopicConfig.TOPIC_GREETING, greeting);
    }

    public void sendMessageWithCompletableFuture(String message) {
        CompletableFuture<SendResult<String, String>> future = kafkaTemplate.send(KafkaTopicConfig.TOPIC_NAME, message);
        future.whenComplete((result, ex) -> {
            if (ex == null) {
                System.out.println("Sent message=[" + message +
                        "] with offset=[" + result.getRecordMetadata().offset() + "]");
            } else {
                System.out.println("Unable to send message=[" +
                        message + "] due to : " + ex.getMessage());
            }
        });
    }

    @PostConstruct
    public void sendMessagesToKafka() {
        sendMessageWithCompletableFuture("hello " +  KafkaTopicConfig.TOPIC_NAME + " " + LocalDateTime.now());
        sendMessage2("hello  " + KafkaTopicConfig.TOPIC_NAME_2 + " " + LocalDateTime.now());
        sendMessage3("hello  " + KafkaTopicConfig.TOPIC_NAME_3 + " " + LocalDateTime.now());
        sendGreetingMessage("greetings-hello");
    }
}
