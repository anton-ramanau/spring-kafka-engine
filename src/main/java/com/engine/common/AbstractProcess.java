package com.engine.common;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public abstract class AbstractProcess {

    private final ObjectMapper objectMapper;

    public AbstractProcess() {
        objectMapper = new ObjectMapper();
    }

    protected String serialize(Object object) {
        try {
            return objectMapper.writeValueAsString(object);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Could not serialize " + object.getClass().getName());
        }
    }

    public <T> T deserialize(String json, Class<T> valueType) {
        try {
            return objectMapper.readValue(json, valueType);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Could not deserialize " + valueType.getName());
        }
    }

}

