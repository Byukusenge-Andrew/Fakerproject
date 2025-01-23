package com.datagenerator;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Scanner;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Main class to handle database insertion, CSV import/export, and multithreaded operations.
 * This class orchestrates the process of generating data, inserting into the database,
 * importing/exporting data from/to CSV using multithreading.
 */
public class insert {
    private static final int BATCH_SIZE = 50000; // Increased batch size for efficiency
    private static final long TOTAL_RECORDS = 10_000_000L; // Configurable total records
    private static long RECORDS_PER_THREAD ; // Default thread count (to be adjusted by user)
    private static final AtomicLong totalRecordsInserted = new AtomicLong(0);
    private static long startTime;
    private static final Scanner scanner = new Scanner(System.in);

    public static void main(String[] args) {
        startTime = System.currentTimeMillis();

        // Ask user for operation choice
        System.out.println("Choose operation:");
        System.out.println("1. Generate and insert data into database");
        System.out.println("2. Import data from CSV into database");
        System.out.println("3. Export data from database to CSV");
        System.out.println("4. Both operations");
        System.out.print("Enter your choice (1-4): ");

        int choice = scanner.nextInt();
        if (choice < 1 || choice > 4) {
            System.out.println("Invalid choice. Exiting...");
            return;
        }

        // Get thread count from user for data generation/import
        int userThreadCount = 0;
        if (choice == 1 || choice == 4) {
            System.out.print("Enter number of threads for data insertion (recommended: 10 or more): ");
            userThreadCount = scanner.nextInt();
            if (userThreadCount < 1) {
                System.out.println("Invalid thread count. Exiting...");
                return;
            }
        }

        // Get thread count from user for CSV import
        int csvImportThreadCount = 0;
        if (choice == 2 || choice == 4) {
            System.out.print("Enter number of threads for CSV import (recommended: 5 or more): ");
            csvImportThreadCount = scanner.nextInt();
            if (csvImportThreadCount < 1) {
                System.out.println("Invalid thread count. Exiting...");
                return;
            }
        }


        int csvExportThreadCount = 0;
        if (choice == 3 || choice == 4) {
            System.out.print("Enter number of threads for CSV export (recommended: 1): ");
            csvExportThreadCount = scanner.nextInt();
            if (csvExportThreadCount < 1) {
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

        ExecutorService   executorService;
        List<Connection> connections = new ArrayList<>();
        CountDownLatch completionLatch = null;

        // Execute based on the user's choice
        if (choice == 1) {
            // Database only with user-specified threads
            executorService = Executors.newFixedThreadPool(userThreadCount);
            completionLatch = new CountDownLatch(userThreadCount);
            RECORDS_PER_THREAD = TOTAL_RECORDS/userThreadCount;
            handleDatabaseOperations(executorService, connections, completionLatch,
                    properties, userThreadCount, RECORDS_PER_THREAD);
        } else if (choice == 2) {
            // Import from CSV with user-specified threads
            executorService = Executors.newFixedThreadPool(csvImportThreadCount);
            completionLatch = new CountDownLatch(csvImportThreadCount); // Initialize completionLatch
            System.out.print("Enter the CSV file path: ");
            String csvFilePath = scanner.next();
            System.out.print("Enter the table name: ");
            String tableName = scanner.next();
            System.out.print("Enter batch size: ");
            int batchSize = scanner.nextInt();

            handleCsvImportOperations(executorService, csvFilePath, tableName, batchSize, csvImportThreadCount, completionLatch, properties);
        } else if (choice == 3) {
            // Export to CSV
            executorService = Executors.newFixedThreadPool(csvExportThreadCount); // Set threads for export
            completionLatch = new CountDownLatch(csvExportThreadCount); // Only one thread for export
            System.out.print("Enter the CSV file path to export to: ");
            String exportCsvPath = scanner.next();
            System.out.println("Enter the table name to get data frrom: ");

            handleCsvExportOperations(exportCsvPath, properties);
            try {
                completionLatch.await();
            } catch (InterruptedException e) {
                System.out.println("Export interrupted: " + e.getMessage());
            }
        } else {
            // Both operations with default thread counts
            executorService = Executors.newFixedThreadPool(userThreadCount + csvImportThreadCount);
            completionLatch = new CountDownLatch(userThreadCount + csvImportThreadCount);
            handleBothOperations(executorService, connections, completionLatch, properties, userThreadCount, csvImportThreadCount);
        }

        // Wait for all tasks to complete using CountDownLatch
        try {
            completionLatch.await(30, TimeUnit.MINUTES);
            executorService.shutdown();

            // Close connections
            for (Connection conn : connections) {
                if (conn != null && !conn.isClosed()) {
                    conn.commit();
                    conn.close();
                }
            }

            // Log performance and data verification
            logProgress(RECORDS_PER_THREAD * userThreadCount);
            System.out.println("Verifying data insertion...");
            verifyDataInsertion(properties);

        } catch (SQLException | InterruptedException e) {
            System.out.println("Error: " + e.getMessage());
        } finally {
            // Cleanup connections in case of error
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

    private static void handleDatabaseOperations(ExecutorService executorService,
                                                 List<Connection> connections, CountDownLatch completionLatch,
                                                 Properties properties, int threadCount, long recordsPerThread) {
        String url = properties.getProperty("db.url");
        String user = properties.getProperty("db.user");
        String password = properties.getProperty("db.password");

        try {
            for (int i = 0; i < threadCount; i++) {
                Connection conn = DriverManager.getConnection(url, user, password);
                conn.setAutoCommit(false);
                connections.add(conn);
                executorService.submit(new DataGenerator(conn, recordsPerThread,
                        BATCH_SIZE, i, totalRecordsInserted, TOTAL_RECORDS, completionLatch));
            }
            System.out.printf("Started %d database threads, %d records per thread%n",
                    threadCount, recordsPerThread);
        } catch (SQLException e) {
            System.out.println("Database connection error: " + e.getMessage());
        }
    }

    private static void handleCsvImportOperations(ExecutorService executorService,
                                                  String csvFilePath, String tableName,
                                                  int batchSize, int threadCount,
                                                  CountDownLatch completionLatch, Properties properties) {
        long recordsPerThread = TOTAL_RECORDS / threadCount;

        try {
            for (int i = 0; i < threadCount; i++) {
                Connection conn = DriverManager.getConnection(properties.getProperty("db.url"),
                        properties.getProperty("db.user"), properties.getProperty("db.password"));
                executorService.submit(new CsvToDatabaseImporter(csvFilePath, conn, tableName, batchSize));
            }
        } catch (SQLException e) {
            System.out.println("Error creating database connection for CSV import: " + e.getMessage());
        }
    }

    private static void handleCsvExportOperations(String exportCsvPath, Properties properties) {
        try (Connection conn = DriverManager.getConnection(properties.getProperty("db.url"),
                properties.getProperty("db.user"), properties.getProperty("db.password"));
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT * FROM try_tb");
             BufferedWriter writer = new BufferedWriter(new FileWriter(exportCsvPath))) {

            int columnCount = rs.getMetaData().getColumnCount();
            while (rs.next()) {
                StringBuilder sb = new StringBuilder();
                for (int i = 1; i <= columnCount; i++) {
                    sb.append(rs.getString(i));
                    if (i < columnCount) {
                        sb.append(",");
                    }
                }
                writer.write(sb.toString());
                writer.newLine();
            }

            System.out.println("Data successfully exported to " + exportCsvPath);

        } catch (SQLException | IOException e) {
            System.out.println("Error during export: " + e.getMessage());
        }
    }

    private static void handleBothOperations(ExecutorService executorService,
                                             List<Connection> connections, CountDownLatch completionLatch,
                                             Properties properties, int dbThreadCount, int csvImportThreadCount) {
        handleDatabaseOperations(executorService, connections, completionLatch, properties, dbThreadCount, RECORDS_PER_THREAD);
        handleCsvImportOperations(executorService, "path/to/csv", "table_name", BATCH_SIZE, csvImportThreadCount, completionLatch, properties);
    }

    private static void logProgress(long count) {
        long currentTime = System.currentTimeMillis();
        double timeInSeconds = (currentTime - startTime) / 1000.0;
        double recordsPerSecond = count / timeInSeconds;
        System.out.printf("Inserted %,d records in %.2f seconds (%.2f records/sec)%n",
                count, timeInSeconds, recordsPerSecond);
    }

    private static void verifyDataInsertion(Properties properties) {
        try (Connection conn = DriverManager.getConnection(properties.getProperty("db.url"),
                properties.getProperty("db.user"), properties.getProperty("db.password"))) {
            String sql = "SELECT COUNT(*) FROM table_name";


        } catch (SQLException e) {
            System.out.println("Error verifying data: " + e.getMessage());
        }
    }
}