package com.datagenerator;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

import com.opencsv.CSVWriter;

/**
 * Exports database records to CSV format using OpenCSV library.
 * This class provides functionality to read data from a database table
 * and write it to a CSV file with proper formatting and error handling.
 * 
 * <p>The class supports:</p>
 * <ul>
 *   <li>Reading from database using JDBC</li>
 *   <li>Writing to CSV with headers</li>
 *   <li>Configurable output path</li>
 *   <li>Progress tracking</li>
 * </ul>
 * 
 * @author Andre Byukusenge
 * @version 1.0
 * @since 2024-03-14
 */
public class ChangeToCsv {
      /** Default output file path */
    private static final String CSVFILE = "csv/people.csv";

    /**
     * Writes the results of a database query to a CSV file.
     * Creates a CSV file with headers and writes all records from the ResultSet.
     * The CSV file will be created in the specified path, creating directories if needed.
     *
     * @param resultSet The ResultSet containing the database query results
     * @throws IOException If there is an error writing to the file
     * @throws SQLException If there is an error reading from the ResultSet
     */
    public static void WriteToCsv(ResultSet resultSet) {
    
     int totalRecords = 0;

     try (CSVWriter fileWriter = new CSVWriter(new FileWriter(CSVFILE),
     CSVWriter.DEFAULT_SEPARATOR,
     CSVWriter.NO_QUOTE_CHARACTER,
     CSVWriter.DEFAULT_ESCAPE_CHARACTER,
     CSVWriter.DEFAULT_LINE_END)) {
           
           String[] header = {"first_name", "last_name", "email"};
           fileWriter.writeNext(header);
             while(resultSet.next()) {
                String[] data = {
                    resultSet.getString("first_name"),
                    resultSet.getString("last_name"),
                    resultSet.getString("email")
                }
                ;
                fileWriter.writeNext(data);
                   totalRecords++;


            }
         System.out.println("CSV file created successfully. Total records: " + totalRecords);


            
        } catch (IOException | SQLException e) {
            System.err.println("SQL Exception: " + e.getMessage());
        }
        
    }

    /**
     * Main method to execute the database to CSV conversion.
     * Reads database configuration from application.properties, executes a query
     * to select all persons, and writes the results to a CSV file.
     *
     * @param args Command line arguments (not used)
     * @throws IOException If properties file cannot be read or CSV file cannot be written
     * @throws SQLException If database operations fail
     */
    public static void main(String[] args) {
        Properties properties = new Properties();
        try {
            properties.load(ChangeToCsv.class.getResourceAsStream("/application.properties"));
        } catch (IOException e) {
            System.err.println("Error loading properties file: " + e.getMessage());
        
        return;
        }

    String url = properties.getProperty("db.url");
    String user = properties.getProperty("db.user");
    String password = properties.getProperty("db.password");


    try(Connection conn = DriverManager.getConnection(url, user, password)) {
        String selectQuery = "SELECT * FROM persons where ";
        PreparedStatement preparedStatement = conn.prepareStatement(selectQuery);
        ResultSet resultSet = preparedStatement.executeQuery();
        WriteToCsv(resultSet);
    } catch(SQLException e) {
        System.err.println("SQL Exception: " + e.getMessage());

    }



    }

}
