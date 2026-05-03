package ru.hse.drip.systems.distributed.homework.i5.protocol;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.core.util.DefaultIndenter;
import com.fasterxml.jackson.core.util.DefaultPrettyPrinter;

public class JsonCodec {
    private final ObjectMapper mapper = new ObjectMapper()
            .findAndRegisterModules()
            .disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);

    public String toLine(Object obj) {
        try {
            return mapper.writeValueAsString(obj);
        } catch (Exception e) {
            throw new IllegalStateException("JSON serialization failed", e);
        }
    }

    public String toPrettyJson(Object obj) {
        try {
            DefaultPrettyPrinter printer = new DefaultPrettyPrinter();
            DefaultIndenter tabIndenter = new DefaultIndenter("\t", System.lineSeparator());
            printer.indentObjectsWith(tabIndenter);
            printer.indentArraysWith(tabIndenter);
            return mapper.writer(printer).writeValueAsString(obj);
        } catch (Exception e) {
            throw new IllegalStateException("JSON serialization failed", e);
        }
    }

    public <T> T fromLine(String line, Class<T> clazz) {
        try {
            return mapper.readValue(line, clazz);
        } catch (Exception e) {
            throw new IllegalArgumentException("Invalid JSON line: " + line, e);
        }
    }
}
