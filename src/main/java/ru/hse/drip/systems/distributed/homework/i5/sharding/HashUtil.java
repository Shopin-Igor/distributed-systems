package ru.hse.drip.systems.distributed.homework.i5.sharding;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public final class HashUtil {
    private HashUtil() {
    }

    public static long sha256ToLong(String value) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] bytes = digest.digest(value.getBytes(StandardCharsets.UTF_8));
            return ByteBuffer.wrap(bytes, 0, Long.BYTES).getLong();
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is not available", e);
        }
    }

    public static int moduloNodeIndex(String key, int nodeCount) {
        if (nodeCount <= 0) {
            throw new IllegalArgumentException("nodeCount must be positive");
        }
        return (int) Long.remainderUnsigned(sha256ToLong(key), nodeCount);
    }
}
