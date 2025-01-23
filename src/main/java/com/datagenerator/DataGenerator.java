package com.datagenerator;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Scanner;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

import com.github.javafaker.Faker;

/**
 * A runnable class that handles data generation and database insertion for a single thread.
 * This class is responsible for generating fake person data and inserting it into
 * the database in batches for optimal performance.
 *
 * @author Andre Byukusenge
 * @version 1.0
 * @since 2024-03-14
 */
public class DataGenerator implements Runnable {
    private final Connection connection;
    private final long recordsToGenerate;  // Changed to long
    private final int batchSize;
    private final int threadId;
    private final AtomicLong totalRecordsInserted;
    private final long maxRecords;         // Changed to long
    private final CountDownLatch completionLatch;
    
    /**
     * Constructs a new DataGenerator with specified parameters.
     *
     * @param connection The database connection to use for this generator
     * @param recordsToGenerate The total number of records this generator should create
     * @param batchSize The number of records to batch together in a single insert
     * @param threadId The identifier for this generator thread
     * @param totalRecordsInserted The shared counter for total records inserted
     * @param maxRecords The maximum number of records to insert
     * @param completionLatch The latch to signal completion
     */
    public DataGenerator(Connection connection, long recordsToGenerate, int batchSize, 
                        int threadId, AtomicLong totalRecordsInserted, long maxRecords,
                        CountDownLatch completionLatch) {
        this.connection = connection;
        this.recordsToGenerate = recordsToGenerate;
        this.batchSize = batchSize;
        this.threadId = threadId;
        this.totalRecordsInserted = totalRecordsInserted;
        this.maxRecords = maxRecords;
        this.completionLatch = completionLatch;
    }

    /**
     * Executes the data generation and insertion process.
     * Generates fake person data using JavaFaker and inserts it into the database
     * in batches. Progress is logged periodically based on the batch size.
     *
     * @throws SQLException If a database access error occurs
     */
    @Override
    public void run() {
        Thread.currentThread().setName("DB-Thread-" + threadId);
        try {
            if (connection == null || connection.isClosed()) {
                System.out.println("Thread " + threadId + ": Invalid database connection");
                return;
            }

            // Check if table exists and has data
            try (var stmt = connection.createStatement();
                 var scanner = new Scanner(System.in)) {
                
                stmt.execute("CREATE TABLE IF NOT EXISTS try_tb (" +
                    "id BIGINT AUTO_INCREMENT PRIMARY KEY," +
                    "first_name VARCHAR(100)," +
                    "last_name VARCHAR(100)," +
                    "email VARCHAR(150))");
                connection.commit();

                // Check for existing data
                var rs = stmt.executeQuery("SELECT COUNT(*) FROM try_tb");
                if (rs.next() && rs.getLong(1) > 0) {
                    System.out.println("Warning: Table try_tb already contains " + rs.getLong(1) + " records");
                    System.out.println("Do you want to continue? (Data will be appended) Y/N");
                    String response = scanner.nextLine().trim().toUpperCase();
                    if (!response.equals("Y")) {
                        System.out.println("Thread " + threadId + ": Aborting due to existing data");
                        return;
                    }
                }
            }

            String insertQuery = "INSERT INTO try_tb (first_name, last_name, email) VALUES (?, ?, ?)";
            Faker faker = new Faker();
            long lastCount = 0;
            
            try (PreparedStatement preparedStatement = connection.prepareStatement(insertQuery)) {
            
            for (long i = 0; i < recordsToGenerate; i++) {  // Changed to long
                // Check if we've reached total limit
                if (totalRecordsInserted.get() >= maxRecords) {
                    System.out.printf("Thread %d: Stopping - Total records limit reached%n", threadId);
                    break;
                }

                preparedStatement.setString(1, faker.name().firstName());
                preparedStatement.setString(2, faker.name().lastName());
                preparedStatement.setString(3, faker.internet().emailAddress());
                preparedStatement.addBatch();
                
                if ((i + 1) % batchSize == 0) {
                    preparedStatement.executeBatch();
                    connection.commit();
                    totalRecordsInserted.addAndGet(batchSize);
                    
                    // Verify data insertion
                    var stmt = connection.createStatement();
                    var rs = stmt.executeQuery("SELECT COUNT(*) FROM try_tb");
                    if (rs.next()) {
                        long currentCount = rs.getLong(1);
                        long newRecords = currentCount - lastCount;
                        System.out.printf("Thread %d: Processed %d records (Verified: %d new records)%n", 
                            threadId, i + 1, newRecords);
                        lastCount = currentCount;
                        
                        if (newRecords != batchSize) {
                            System.out.printf("Warning: Thread %d - Expected %d records but inserted %d%n",
                                threadId, batchSize, newRecords);
                        }
                    }
                    
                    // Add memory usage info
                    Runtime rt = Runtime.getRuntime();
                    long usedMemory = (rt.totalMemory() - rt.freeMemory()) / 1024 / 1024;
                    System.out.printf("Thread %d: Processed %d records (Memory used: %dMB)%n", 
                        threadId, i + 1, usedMemory);
                    
                    // Enhanced progress reporting
                    double progress = (i + 1.0) / recordsToGenerate * 100;
                    System.out.printf("DB Thread %d: %.2f%% complete (Memory: %dMB)%n", 
                        threadId, progress, usedMemory);
                }
            }
            
            // Process and verify remaining records
            preparedStatement.executeBatch();
            connection.commit();
            
            var stmt = connection.createStatement();
            var rs = stmt.executeQuery("SELECT COUNT(*) FROM try_tb");
            if (rs.next()) {
                long finalCount = rs.getLong(1);
                System.out.printf("Thread %d: Final verification - Total records: %d%n", 
                    threadId, finalCount);
            }
            
            }
            
        } catch (SQLException e) {
            System.out.println("Error in thread " + threadId + ": " + e.getMessage());
        } finally {
            completionLatch.countDown();
        }
    }
}
