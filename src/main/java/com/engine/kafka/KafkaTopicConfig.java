package com.engine.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.aop.support.AopUtils;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Configuration;

import javax.annotation.PostConstruct;
import java.lang.reflect.Field;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

@Configuration
@Slf4j
public class KafkaTopicConfig {

    private final KafkaConfig kafkaConfig;
    private final ApplicationContext context;

    public KafkaTopicConfig(KafkaConfig kafkaConfig, ApplicationContext context) {
        this.kafkaConfig = kafkaConfig;
        this.context = context;
    }

    @PostConstruct
    public void updateTopics() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaConfig.bootstrapAddress);

        try (AdminClient adminClient = AdminClient.create(configs)) {

//          Find declared topic names
            List<String> requireTopicNames = new ArrayList<>();
            Map<String, Object> requireTopicBeans = context.getBeansWithAnnotation(HasTopics.class);
            requireTopicBeans.values().forEach(bean -> {
                for (Field declaredField : AopUtils.getTargetClass(bean).getDeclaredFields()) {
                    if (declaredField.isAnnotationPresent(TopicName.class)) {
                        try {
                            String declaredTopicName = (String) declaredField.get(bean);
                            requireTopicNames.add(declaredTopicName);
                        } catch (IllegalAccessException e) {
                            throw new RuntimeException();
                        }
                    }
                }
            });

//          Find class names duplicates
            if (requireTopicNames.stream().distinct().count() != requireTopicNames.size()) {
                throw new RuntimeException("Processes tries to crate topics with the same name. Topics require to " +
                        "create " + requireTopicNames);
            }

            Set<String> existingTopics = adminClient.listTopics().names().get();
            Set<NewTopic> topicsToCreate = new HashSet<>();

            requireTopicNames.forEach(topicName -> {
                if (existingTopics.contains(topicName)) {
                    existingTopics.remove(topicName);
                } else {
                    topicsToCreate.add(new NewTopic(topicName, 1, (short) 1));
                }
            });

            if (!topicsToCreate.isEmpty()) {
                log.info("Create topics " + topicsToCreate);
                adminClient.createTopics(topicsToCreate);
            }

            if (!existingTopics.isEmpty()) {
                boolean topicsDeleted = false;
                do {
                    log.info("Try to delete not usable topic " + existingTopics);
                    adminClient.deleteTopics(existingTopics);
                    Thread.sleep(1000);
                    topicsDeleted =
                            adminClient.listTopics().names().get().stream().filter(existingTopics::contains).collect(
                                    Collectors.toSet()).isEmpty();
                } while (!topicsDeleted);
            }

        } catch (ExecutionException e) {
            log.error("Topic exception " + e.getMessage());
        } catch (InterruptedException e) {
            log.error(e.getMessage());
        }
    }
}
