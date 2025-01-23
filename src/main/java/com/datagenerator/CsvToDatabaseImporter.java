package com.datagenerator;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class CsvToDatabaseImporter implements Runnable {
    private final String csvFilePath;
    private final Connection connection;
    private final String tableName;
    private final int batchSize;

    public CsvToDatabaseImporter(String csvFilePath, Connection connection, String tableName, int batchSize) {
        this.csvFilePath = csvFilePath;
        this.connection = connection;
        this.tableName = tableName;
        this.batchSize = batchSize;
    }

    @Override
    public void run() {
        String insertQuery = String.format("INSERT INTO %s (id, first_name, last_name, email) VALUES (?, ?, ?, ?)", tableName);

        try (BufferedReader br = new BufferedReader(new FileReader(csvFilePath));
             PreparedStatement pstmt = connection.prepareStatement(insertQuery)) {

            String line;
            int count = 0;

            // Skip the header row
            br.readLine();

            while ((line = br.readLine()) != null) {
                String[] values = line.split(","); // Assuming the CSV is comma-separated

                // Ensure the line has the correct number of columns
                if (values.length < 4) {
                    System.err.println("Skipping malformed row: " + line);
                    continue;
                }

                // Map values from the CSV to the columns in the database
                try {
                    pstmt.setInt(1, Integer.parseInt(values[0].trim())); // id
                } catch (NumberFormatException e) {
                    System.err.println("Skipping row due to invalid id value: " + values[0]);
                    continue;
                }

                pstmt.setString(2, values[1].trim()); // first_name
                pstmt.setString(3, values[2].trim()); // last_name
                pstmt.setString(4, values[3].trim()); // email

                pstmt.addBatch(); // Add the operation to a batch

                // Execute the batch after every 'batchSize' rows
                if (++count % batchSize == 0) {
                    pstmt.executeBatch();
                    System.out.println("Inserted " + count + " rows into the database.");
                }
            }

            // Execute any remaining rows in the batch
            if (count % batchSize != 0) {
                pstmt.executeBatch();
                System.out.println("Inserted remaining " + count % batchSize + " rows into the database.");
            }

            System.out.println("Data imported successfully from " + csvFilePath);

        } catch (IOException e) {
            System.err.println("Error reading CSV file: " + e.getMessage());
        } catch (SQLException e) {
            System.err.println("Error inserting data into database: " + e.getMessage());
        }
    }
}
