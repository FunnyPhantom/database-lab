package common;

import java.sql.*;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class Statics {

    // CHANGE THIS TO LOCAL DATABASE SETTING.
    public static final String URL_CONNECTION = "jdbc:mysql://localhost/university?" +
            "user=n1&password=123456@a&useSSL=false&allowPublicKeyRetrieval=true";
    public static void showQueryResults(String query, String urlConnection) {
        try (
//                Class.forName("com.mysql.jdbc.Driver");
                Connection connection = DriverManager.getConnection(
                        urlConnection)) {
            try (Statement statement = connection.createStatement();
                 ResultSet rs = statement.executeQuery(query)) {

                showQueryResults(rs);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
    public static void showQueryResults(String query) {
        showQueryResults(query, URL_CONNECTION);
    }
    public static void showQueryResults(ResultSet rs) {
        try {
            ResultSetMetaData metaData = rs.getMetaData();

            System.out.print("     ");
            for (int c = 1; c <= metaData.getColumnCount(); c++) {
                System.out.printf("%-30s", metaData.getColumnName(c));
            }
            System.out.println();

            int i = 1;
            while (rs.next()) {
                System.out.printf("[%2d] ", i++);
                for (int c = 1; c <= metaData.getColumnCount(); c++) {
                    System.out.printf("%-30s", rs.getString(c));
                }
                System.out.println();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
    public static void showQueryResults(ResultSet rs, Collection<Integer> columns) {
        try {
            ResultSetMetaData metaData = rs.getMetaData();

            System.out.print("     ");
            for (int c = 1; c <= metaData.getColumnCount(); c++) {
                if (columns.contains(c)) {
                    System.out.printf("%-30s", metaData.getColumnName(c));
                }
            }
            System.out.println();

            int i = 1;
            while (rs.next()) {
                System.out.printf("[%2d] ", i++);
                for (int c = 1; c <= metaData.getColumnCount(); c++) {
                    if (columns.contains(c)) {
                        System.out.printf("%-30s", rs.getString(c));
                    }
                }
                System.out.println();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
