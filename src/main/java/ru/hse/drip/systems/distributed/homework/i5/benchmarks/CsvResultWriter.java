package ru.hse.drip.systems.distributed.homework.i5.benchmarks;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

public class CsvResultWriter {
    public void write(Path output, List<BenchmarkResult> results) throws IOException {
        Files.createDirectories(output.getParent());
        StringBuilder csv = new StringBuilder();
        if (results.isEmpty()) {
            csv.append(new BenchmarkResult().csvHeader()).append(System.lineSeparator());
        } else {
            csv.append(results.getFirst().csvHeader()).append(System.lineSeparator());
            for (BenchmarkResult result : results) {
                csv.append(result.toCsvRow()).append(System.lineSeparator());
            }
        }
        Files.writeString(output, csv.toString(), StandardCharsets.UTF_8);
    }
}
