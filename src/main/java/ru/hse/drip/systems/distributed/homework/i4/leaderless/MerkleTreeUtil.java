package ru.hse.drip.systems.distributed.homework.i4.leaderless;

import ru.hse.drip.systems.distributed.homework.i4.protocol.MerkleRecord;
import ru.hse.drip.systems.distributed.homework.i4.protocol.VersionedValue;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public final class MerkleTreeUtil {
    public static final int BUCKET_COUNT = 16;

    private MerkleTreeUtil() {}

    public static int bucketOf(String key) {
        return Math.floorMod(key == null ? 0 : key.hashCode(), BUCKET_COUNT);
    }

    public static Map<Integer, String> bucketHashes(Map<String, VersionedValue> snapshot) {
        Map<Integer, List<MerkleRecord>> grouped = new HashMap<>();
        for (int bucket = 0; bucket < BUCKET_COUNT; bucket++) {
            grouped.put(bucket, new ArrayList<>());
        }

        for (var entry : snapshot.entrySet()) {
            VersionedValue value = entry.getValue();
            grouped.get(bucketOf(entry.getKey())).add(new MerkleRecord(
                    entry.getKey(),
                    value == null ? null : value.value,
                    value == null ? null : value.version,
                    value == null ? null : value.operationId
            ));
        }

        Map<Integer, String> hashes = new TreeMap<>();
        for (int bucket = 0; bucket < BUCKET_COUNT; bucket++) {
            List<MerkleRecord> records = grouped.get(bucket);
            records.sort((left, right) -> left.key.compareTo(right.key));
            StringBuilder serialized = new StringBuilder();
            for (MerkleRecord record : records) {
                serialized.append(record.key).append('|')
                        .append(record.value == null ? "" : record.value).append('|')
                        .append(record.version == null ? 0 : record.version.lamport).append('|')
                        .append(record.version == null || record.version.nodeId == null ? "" : record.version.nodeId).append('|')
                        .append(record.operationId == null ? "" : record.operationId);
            }
            hashes.put(bucket, sha256(serialized.toString()));
        }
        return hashes;
    }

    public static String rootHash(Map<Integer, String> bucketHashes) {
        List<String> level = new ArrayList<>();
        for (int bucket = 0; bucket < BUCKET_COUNT; bucket++) {
            level.add(bucketHashes.getOrDefault(bucket, sha256("")));
        }

        while (level.size() > 1) {
            List<String> next = new ArrayList<>();
            for (int i = 0; i < level.size(); i += 2) {
                next.add(sha256(level.get(i) + level.get(i + 1)));
            }
            level = next;
        }
        return level.getFirst();
    }

    public static List<MerkleRecord> recordsForBuckets(Map<String, VersionedValue> snapshot, List<Integer> bucketIndexes) {
        List<MerkleRecord> records = new ArrayList<>();
        for (var entry : snapshot.entrySet()) {
            int bucket = bucketOf(entry.getKey());
            if (!bucketIndexes.contains(bucket)) {
                continue;
            }
            VersionedValue value = entry.getValue();
            records.add(new MerkleRecord(entry.getKey(), value.value, value.version, value.operationId));
        }
        records.sort((left, right) -> left.key.compareTo(right.key));
        return records;
    }

    private static String sha256(String value) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] bytes = digest.digest(value.getBytes(StandardCharsets.UTF_8));
            StringBuilder hex = new StringBuilder(bytes.length * 2);
            for (byte b : bytes) {
                hex.append(String.format("%02x", b));
            }
            return hex.toString();
        } catch (Exception e) {
            throw new IllegalStateException("Не удалось вычислить SHA-256", e);
        }
    }
}
