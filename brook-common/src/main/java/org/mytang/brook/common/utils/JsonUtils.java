package org.mytang.brook.common.utils;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES;
import static com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNRESOLVED_OBJECT_IDS;

public abstract class JsonUtils {

    private static final ObjectMapper DEFAULT = new ObjectMapper()
            .configure(FAIL_ON_UNKNOWN_PROPERTIES, false)
            .configure(FAIL_ON_UNRESOLVED_OBJECT_IDS, false)
            .setSerializationInclusion(JsonInclude.Include.NON_NULL);

    private static volatile ObjectMapper objectMapper = DEFAULT;

    public static void setObjectMapper(ObjectMapper objectMapper) {
        JsonUtils.objectMapper = Optional.ofNullable(objectMapper).orElse(DEFAULT);
    }

    public static String toJsonString(Object object) {
        Objects.requireNonNull(objectMapper);
        try {
            return objectMapper.writeValueAsString(object);
        } catch (JsonProcessingException je) {
            throw new RuntimeException(je);
        }
    }

    public static <T> T readValue(String value, final Class<T> valueType) {
        Objects.requireNonNull(objectMapper);
        try {
            return objectMapper.readValue(value, valueType);
        } catch (IOException je) {
            throw new RuntimeException(je);
        }
    }

    public static <T> T readValue(byte[] value, final Class<T> valueType) {
        Objects.requireNonNull(objectMapper);
        try {
            return objectMapper.readValue(value, valueType);
        } catch (IOException je) {
            throw new RuntimeException(je);
        }
    }

    public static <T> T readValue(String value, final TypeReference<T> valueTypeRef) {
        Objects.requireNonNull(objectMapper);
        try {
            return objectMapper.readValue(value, valueTypeRef);
        } catch (IOException je) {
            throw new RuntimeException(je);
        }
    }

    public static <T> T readValue(byte[] value, final TypeReference<T> valueTypeRef) {
        Objects.requireNonNull(objectMapper);
        try {
            return objectMapper.readValue(value, valueTypeRef);
        } catch (IOException je) {
            throw new RuntimeException(je);
        }
    }

    public static <T> T convertValue(Object fromValue, final Class<T> toValueType) {
        Objects.requireNonNull(objectMapper);
        return objectMapper.convertValue(fromValue, toValueType);
    }

    public static <T> T convertValue(Object fromValue, final TypeReference<T> toValueTypeRef) {
        Objects.requireNonNull(objectMapper);
        return objectMapper.convertValue(fromValue, toValueTypeRef);
    }

    public static <T> T convertValue(Object fromValue, final Type toValueType) {
        Objects.requireNonNull(objectMapper);
        return objectMapper.convertValue(fromValue,
                objectMapper.getTypeFactory().constructType(toValueType));
    }

    public static byte[] writeValueAsBytes(Object value) {
        Objects.requireNonNull(objectMapper);
        try {
            return objectMapper.writeValueAsBytes(value);
        } catch (JsonProcessingException je) {
            throw new RuntimeException(je);
        }
    }

    public static Object parse(String json) {
        try {
            return convert(objectMapper.readTree(json));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static Object parse(InputStream inputStream) {
        try {
            return convert(objectMapper.readTree(inputStream));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static Object convert(JsonNode node) {
        if (node.isObject()
                || node.isPojo()) {
            return objectMapper.convertValue(node,
                    new TypeReference<Map<String, Object>>() {
                    });
        } else if (node.isArray()) {
            return objectMapper.convertValue(node,
                    new TypeReference<List<Object>>() {
                    });
        } else if (node.isInt()
                || node.isShort()) {
            return node.asInt();
        } else if (node.isLong()) {
            return node.asLong();
        } else if (node.isFloat()
                || node.isDouble()) {
            return node.asDouble();
        } else if (node.isBoolean()) {
            return node.asBoolean();
        } else if (node.isValueNode()) {
            return node.asText();
        } else {
            return node.toString();
        }
    }
}
