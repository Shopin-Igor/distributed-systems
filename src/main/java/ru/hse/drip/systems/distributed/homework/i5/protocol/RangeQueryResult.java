package ru.hse.drip.systems.distributed.homework.i5.protocol;

import java.util.LinkedHashMap;
import java.util.Map;

public class RangeQueryResult {
    public Map<String, String> values = new LinkedHashMap<>();

    public RangeQueryResult() {
    }

    public RangeQueryResult(Map<String, String> values) {
        this.values = new LinkedHashMap<>(values);
    }
}
