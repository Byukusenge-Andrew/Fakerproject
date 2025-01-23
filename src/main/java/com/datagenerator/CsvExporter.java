package com.datagenerator;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.concurrent.CountDownLatch;

import com.github.javafaker.Faker;

/**
 * Handles parallel CSV data generation and export.
 * Generates the same type of fake data as database insertion but writes to CSV file.
 */
public class CsvExporter implements Runnable {
    private static final Object WRITE_LOCK = new Object();
    private static volatile boolean headerWritten = false;
    private final String filePath;
    private final long recordsToGenerate;  // Changed to long
    private final int batchSize;
    private final CountDownLatch completionLatch;
    private final int threadId;

    public CsvExporter(String filePath, long recordsToGenerate, int batchSize,
                       CountDownLatch completionLatch, int threadId) {
        this.filePath = filePath;
        this.recordsToGenerate = recordsToGenerate;
        this.batchSize = batchSize;
        this.completionLatch = completionLatch;
        this.threadId = threadId;
    }

    @Override
    public void run() {
        try {
            Faker faker = new Faker();
            StringBuilder batch = new StringBuilder();

            // Write header if first thread
            synchronized(WRITE_LOCK) {
                if (!headerWritten) {
                    try (PrintWriter writer = new PrintWriter(new FileWriter(filePath))) {
                        writer.println("first_name,last_name,email");
                        headerWritten = true;
                    }
                }
            }

            for (long i = 0; i < recordsToGenerate; i++) {  // Changed to long
                String line = String.format("%s,%s,%s%n",
                        faker.name().firstName(),
                        faker.name().lastName(),
                        faker.internet().emailAddress());

                batch.append(line);

                if ((i + 1) % batchSize == 0) {
                    // Synchronized batch write
                    synchronized(WRITE_LOCK) {
                        try (PrintWriter writer = new PrintWriter(new FileWriter(filePath, true))) {
                            writer.write(batch.toString());
                        }
                        // Add progress percentage
                        double progress = (i + 1.0) / recordsToGenerate * 100;
                        System.out.printf("CSV Thread %d: %.2f%% complete%n",
                                threadId, progress);
                    }
                    batch.setLength(0);
                    System.out.printf("CSV Export Thread %d: Processed %d records%n",
                            threadId, i + 1);
                }
            }

            // Write remaining records
            if (batch.length() > 0) {
                synchronized(WRITE_LOCK) {
                    try (PrintWriter writer = new PrintWriter(new FileWriter(filePath, true))) {
                        writer.write(batch.toString());
                    }
                }
            }

        } catch (IOException e) {
            System.out.println("CSV Export Error in thread " + threadId + ": " + e.getMessage());
        } finally {
            completionLatch.countDown();
        }
    }
}
