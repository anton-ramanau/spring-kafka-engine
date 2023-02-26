package com.engine.kafka;

import com.engine.common.ProcessMessage;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class TopicMessageService {

    private final KafkaMessageService kafkaMessageService;
    private final ObjectMapper objectMapper;

    public TopicMessageService(KafkaMessageService kafkaMessageService,
                                ObjectMapper objectMapper) {
        this.kafkaMessageService = kafkaMessageService;
        this.objectMapper = objectMapper;
    }

    public void sendMessage(String topic, ProcessMessage processMessage) {
        try {
            String json = objectMapper.writeValueAsString(processMessage);
            kafkaMessageService.sendMessage(topic, json);
        } catch (JsonProcessingException e) {
            log.error(e.getMessage());
        }
    }
}
