package com.engine.kafka;

/**
 * Is an interface for listening, receiving messages and executing particular process.
 * Implementation should provide functionality for listening any resource.
 */
public interface TopicListener {

    /**
     * @param message - message for that is service listening.
     */
    void listenForMessage(String message);
}
