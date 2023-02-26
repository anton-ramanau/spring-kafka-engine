package com.engine.kafka;

public interface KafkaMessageService {

    void sendMessage(String kafkaTopic, String message);
}
