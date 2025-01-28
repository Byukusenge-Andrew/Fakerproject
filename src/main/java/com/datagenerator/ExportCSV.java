package com.datagenerator;

import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

import com.opencsv.CSVWriter;

public class ExportCSV {

    private static final String DB_URL = "jdbc:mysql://localhost:3306/people_db?useSSL=false&allowPublicKeyRetrieval=true&rewriteBatchedStatements=true";
    private static final String DB_USER = "root";
    private static final String DB_PASSWORD = "root";
    private static final int FETCH_SIZE = 1000000;
    private static final int THREAD_COUNT = 10;
// ... existing code ...

    public static void main(String[] args) {
        String csvFile = "exportPeople2.csv";
        String backupTable = "people_backup";  // Name of the backup table in the database
        ExecutorService executorService = Executors.newFixedThreadPool(THREAD_COUNT);
        ExecutorService backupExecutor = Executors.newFixedThreadPool(THREAD_COUNT);
        LinkedBlockingQueue<String[]> queue = new LinkedBlockingQueue<>();

        // Track progress variables
        final long totalRecords = 10000000;  // Estimated total records (you can fetch this value dynamically if needed)
        final long[] recordsProcessed = {0};  // Use an array to update the value in lambda expressions

        // Record the start time
        final long startTime = System.currentTimeMillis(); // Initialize startTime here

        // Ensure CSV file is created
        try (CSVWriter csvWriter = new CSVWriter(new FileWriter(csvFile, true))) {
            csvWriter.writeNext(new String[]{"ID", "Name", "Email", "Address", "Age"});
        } catch (IOException e) {
            e.printStackTrace();
            return;
        }

        // Thread to write to the CSV file
        Thread writerThread = new Thread(() -> {
            try (CSVWriter csvWriter = new CSVWriter(new FileWriter(csvFile, true))) {
                while (true) {
                    String[] data = queue.take();
                    if (data.length == 0) break; // Exit signal
                    csvWriter.writeNext(data);
                    synchronized (recordsProcessed) {
                        recordsProcessed[0]++;
                    }
                }
            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
            }
        });
        writerThread.start();

        // Thread to insert data into the backup table
        try (Connection connection = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD)) {
            System.out.println("Database connection has been established.");
            connection.setAutoCommit(false);

            // Create table if it doesn't exist
            try (PreparedStatement createTableStmt = connection.prepareStatement(
                    "CREATE TABLE IF NOT EXISTS people2 (" +
                            "id SERIAL PRIMARY KEY, " +
                            "name VARCHAR(255), " +
                            "email VARCHAR(255), " +
                            "address VARCHAR(255), " +
                            "age INT)")) {
                createTableStmt.execute();
            }

            // Insert data into the table using generate_series
            try (PreparedStatement insertStmt = connection.prepareStatement(
                    "INSERT INTO people2 (name, email, address, age) " +
                            "SELECT 'Name' || gs.id, 'email' || gs.id || '@example.com', 'Address' || gs.id, 20 + (gs.id % 50) " +
                            "FROM generate_series(1, 10000000) AS gs(id)")) {
                insertStmt.execute();
            }
            String query = "SELECT seq.id AS id, " +
                    "COALESCE(p.name, '') AS name, " +
                    "COALESCE(p.email, '') AS email, " +
                    "COALESCE(p.address, '') AS address, " +
                    "COALESCE(p.age, 0) AS age " +
                    "FROM ( " +
                    "  SELECT @curRow := @curRow + 1 AS id " +
                    "  FROM (SELECT @curRow := 0) r " +
                    "  CROSS JOIN (SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4) d1 " +
                    "  CROSS JOIN (SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4) d2 " +
                    "  CROSS JOIN (SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4) d3 " +
                    "  CROSS JOIN (SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4) d4 " +
                    "  CROSS JOIN (SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4) d5 " +
                    "  CROSS JOIN (SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4) d6 " +
                    ") seq " +
                    "LEFT JOIN people2 p ON seq.id = p.id " +
                    "ORDER BY seq.id " +
                    "LIMIT 10000000;";

            try (PreparedStatement preparedStatement = connection.prepareStatement(query,
                    ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
                preparedStatement.setFetchSize(FETCH_SIZE);

                try (ResultSet resultSet = preparedStatement.executeQuery()) {
                    while (resultSet.next()) {
                        String id = String.valueOf(resultSet.getInt("id"));
                        String name = resultSet.getString("name");
                        String email = resultSet.getString("email");
                        String address = resultSet.getString("address");
                        String age = String.valueOf(resultSet.getInt("age"));

                        String[] data = new String[]{id, name, email, address, age};

                        // Submit task for writing to CSV
                        executorService.submit(() -> {
                            try {
                                queue.put(data);
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                            }
                        });

                        // Submit task for backup insert
                        backupExecutor.submit(() -> {
                            try (PreparedStatement backupInsertStmt = connection.prepareStatement(
                                    "INSERT INTO " + backupTable + " (id, name, email, address, age) VALUES (?, ?, ?, ?, ?)")) {
                                backupInsertStmt.setInt(1, Integer.parseInt(id));
                                backupInsertStmt.setString(2, name);
                                backupInsertStmt.setString(3, email);
                                backupInsertStmt.setString(4, address);
                                backupInsertStmt.setInt(5, Integer.parseInt(age));
                                backupInsertStmt.executeUpdate();
                            } catch (SQLException e) {
                                e.printStackTrace();
                            }
                        });

                        // Print progress every 1000 records
                        synchronized (recordsProcessed) {
                            if (recordsProcessed[0] % 1000000 == 0) {
                                long recordsDone = recordsProcessed[0];
                                long timeElapsed = System.currentTimeMillis() - startTime;  // Using the startTime now
                                double elapsedSeconds = timeElapsed / 1000.0;
                                double estimatedTimeLeft = ((totalRecords - recordsDone) / (recordsDone + 1)) * elapsedSeconds;
                                System.out.printf("Processed %d records. Time left: %.2f seconds%n", recordsDone, estimatedTimeLeft);
                            }
                        }
                    }
                }
            }

            // Shutdown both executors
            executorService.shutdown();
            backupExecutor.shutdown();

            // Wait until all tasks are finished
            while (!executorService.isTerminated() || !backupExecutor.isTerminated()) {
                // Waiting for all tasks to finish
            }
            queue.put(new String[0]);  // Signal the writer thread to finish
            writerThread.join();  // Wait for the writer thread to finish

            System.out.println("Data was successfully exported to " + csvFile);
            System.out.println("Data was successfully backed up to the " + backupTable);

        } catch (SQLException | InterruptedException e) {
            e.printStackTrace();
        }
    }

}