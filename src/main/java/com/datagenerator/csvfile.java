package com.datagenerator;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Scanner;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class csvfile {
    /**
     * Main method to orchestrate multithreaded data generation and insertion.
     * Creates multiple database connections and threads to parallelize the work.
     *
     * <p>Process overview:</p>
     * <ol>
     *   <li>Loads database configuration</li>
     *   <li>Creates multiple database connections</li>
     *   <li>Spawns worker threads for data generation</li>
     *   <li>Monitors progress and manages transactions</li>
     *   <li>Aggregates results and reports performance metrics</li>
     * </ol>
     *
     * @param args Command line arguments (not used)
     * @throws IOException If the properties file cannot be read
     * @throws SQLException If database operations fail
     */

     private static final int BATCH_SIZE = 5000; // Increased batch size
     private static final int BASE_THREAD_COUNT = Runtime.getRuntime().availableProcessors();
     private static final int ADDITIONAL_THREADS = 3;
     private static final int THREAD_COUNT = BASE_THREAD_COUNT + ADDITIONAL_THREADS;
     private static final long TOTAL_RECORDS = 10_000_000L; // Configurable total records (e.g., 50M)
     private static final long RECORDS_PER_THREAD = TOTAL_RECORDS / THREAD_COUNT;
     private static final int CSV_THREAD_COUNT = 7; // Increased from 4 to 7
     private static final AtomicLong totalRecordsInserted = new AtomicLong(0);
     private static long startTime;
     private static final Scanner scanner = new Scanner(System.in);
    public static void main(String[] args) {
        startTime = System.currentTimeMillis();
        
        // Ask user for operation choice
        System.out.println("Choose operation:");
        System.out.println("1. Generate and insert data into database");
        System.out.println("2. Export data to CSV");
        System.out.println("3. Both operations");
        System.out.print("Enter your choice (1-3): ");
        
        int choice = scanner.nextInt();
        if (choice < 1 || choice > 3) {
            System.out.println("Invalid choice. Exiting...");
            return;
        }

        int userThreadCount = 0;
        if (choice == 1 || choice == 2) {
            System.out.print("Enter number of threads (recommended: " + 
                Runtime.getRuntime().availableProcessors() + " or more): ");
            userThreadCount = scanner.nextInt();
            if (userThreadCount < 1) {
                System.out.println("Invalid thread count. Exiting...");
                return;
            }
        }

        Properties properties = new Properties();
        try {
            properties.load(insert.class.getResourceAsStream("/application.properties"));
        } catch (IOException e) {
            System.out.println("Error loading properties: " + e.getMessage());
            return;
        }

        ExecutorService executorService;
        List<Connection> connections = new ArrayList<>();
        CountDownLatch completionLatch;
        
        if (choice == 1) {
            System.out.println("Enter only two;");
            return ;
            
        } else if (choice == 2) {
            
            executorService = Executors.newFixedThreadPool(userThreadCount);
            completionLatch = new CountDownLatch(userThreadCount);
            long recordsPerThread = TOTAL_RECORDS / userThreadCount;
            handleCsvOperations(executorService, completionLatch, properties, 
                userThreadCount, recordsPerThread);
        } else {
          
            executorService = Executors.newFixedThreadPool(THREAD_COUNT + CSV_THREAD_COUNT);
            completionLatch = new CountDownLatch(THREAD_COUNT + CSV_THREAD_COUNT);
            handleBothOperations(executorService, connections, completionLatch, properties);
        }

        // Wait for all tasks to complete using CountDownLatch
        try {
            completionLatch.await(30, TimeUnit.MINUTES);
            executorService.shutdown();

            // Remove CSV merge operation as it's no longer needed

            // Close connections
            for (Connection conn : connections) {
                if (conn != null && !conn.isClosed()) {
                    conn.commit();
                    conn.close();
                }
            }

            // Wait for CSV export to complete
            logProgress(RECORDS_PER_THREAD * THREAD_COUNT);
            
            // Add verification after completion
            System.out.println("Verifying data insertion...");
            try (Connection conn = DriverManager.getConnection(properties.getProperty("db.url"), 
                    properties.getProperty("db.user"), properties.getProperty("db.password"))) {
                var stmt = conn.createStatement();
                var rs = stmt.executeQuery("SELECT COUNT(*) FROM try_tb");
                if (rs.next()) {
                    System.out.println("Total records in database: " + rs.getLong(1));
                }
            }
            
            
            System.out.println("\nVerifying final data insertion...");
            try (Connection conn = DriverManager.getConnection(properties.getProperty("db.url"), 
                    properties.getProperty("db.user"), properties.getProperty("db.password"))) {
                var stmt = conn.createStatement();
                
            
                var rs = stmt.executeQuery("SELECT COUNT(*) FROM try_tb");
                if (rs.next()) {
                    long totalRecords = rs.getLong(1);
                    System.out.println("Total records in database: " + totalRecords);
                    
                    if (totalRecords != TOTAL_RECORDS) {
                        System.out.printf("Warning: Expected %d records but found %d%n",
                            TOTAL_RECORDS, totalRecords);
                    }
                }
                
          
                rs = stmt.executeQuery("SELECT * FROM try_tb LIMIT 5");
                System.out.println("\nSample records:");
                while (rs.next()) {
                    System.out.printf("ID: %d, Name: %s %s, Email: %s%n",
                        rs.getLong("id"),
                        rs.getString("first_name"),
                        rs.getString("last_name"),
                        rs.getString("email"));
                }
            }
            
        } catch (SQLException | InterruptedException e) {
            System.out.println("Error: " + e.getMessage());
            
        } catch (IllegalArgumentException | SecurityException | IllegalStateException e) {
            System.out.println("Error: " + e.getMessage());   
            executorService.shutdownNow();
        } finally {
            
            for (Connection conn : connections) {
                try {
                    if (conn != null && !conn.isClosed()) {
                        conn.close();
                    }
                } catch (SQLException e) {
                    System.out.println("Error closing connection: " + e.getMessage());
                }
            }
        }
    }

    private static void handleCsvOperations(ExecutorService executorService, 
            CountDownLatch completionLatch, Properties properties, 
            int threadCount, long recordsPerThread) {
        String csvPath = properties.getProperty("csv.export.path");
        int csvBatchSize = Integer.parseInt(properties.getProperty("csv.batch.size", "10000"));

        for (int i = 0; i < threadCount; i++) {
            executorService.submit(new CsvExporter(csvPath, recordsPerThread, 
                csvBatchSize, completionLatch, i));
        }
        System.out.printf("Started %d CSV export threads, %d records per thread%n", 
            threadCount, recordsPerThread);
    }

    private static void handleBothOperations(ExecutorService executorService, 
            List<Connection> connections, CountDownLatch completionLatch, Properties properties) {
     
        handleCsvOperations(executorService, completionLatch, properties, CSV_THREAD_COUNT, TOTAL_RECORDS / CSV_THREAD_COUNT);
    }

    /**
     * Logs the progress of data insertion including performance metrics.
     *
     * @param count The total number of records processed
     */
    private static void logProgress(long count) {
        long currentTime = System.currentTimeMillis();
        double timeInSeconds = (currentTime - startTime) / 1000.0;
        double recordsPerSecond = count / timeInSeconds;
        System.out.printf("Inserted %,d records in %.2f seconds (%.2f records/sec)%n", 
            count, timeInSeconds, recordsPerSecond);
    }

    public static long getTotalRecordsInserted() {
        return totalRecordsInserted.get();
    }
}





