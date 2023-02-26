package com.engine.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.listener.ErrorHandler;
import org.springframework.kafka.listener.SeekToCurrentErrorHandler;
import org.springframework.util.backoff.BackOff;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
@Slf4j
public class KafkaRetryConfig {

    @Bean
    public ErrorHandler errorHandler() {
        BackOff backOff = new FixedBackOff(4000, 4);
        SeekToCurrentErrorHandler errorHandler = new SeekToCurrentErrorHandler((data, exception) -> {
            String message = "Error: " + exception.getMessage() + "; \n" + "Error message: " + data.toString();
            log.error(message);
            log.error(ExceptionUtils.getStackTrace(exception));
        }, backOff);

        return errorHandler;
    }

}
