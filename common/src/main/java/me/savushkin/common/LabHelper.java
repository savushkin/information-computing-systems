package me.savushkin.common;

import java.sql.*;
import java.util.Properties;

public class LabHelper {
    public static void registration(String className) throws ClassNotFoundException {
        Class.forName(className);
    }

    public static void registration(Driver driver) throws SQLException {
        DriverManager.registerDriver(driver);
    }

    public static Connection connection(String endpoint) throws SQLException {
        return DriverManager.getConnection(endpoint);
    }

    public static Connection connection(String endpoint, String username, String password) throws SQLException {
        return DriverManager.getConnection(endpoint, username, password);
    }

    public static Connection connection(String endpoint, Properties properties) throws SQLException {
        return DriverManager.getConnection(endpoint, properties);
    }

    public static void printResultSet(ResultSet resultSet) throws SQLException {
        ResultSetMetaData metaData = resultSet.getMetaData();

        while(resultSet.next()) {
            int columnsCount = metaData.getColumnCount();
            for (int column = 1; column <= columnsCount; column++) {
                StringBuilder stringBuilder = new StringBuilder();
                stringBuilder.append(metaData.getColumnName(column)).append(": ");
                switch (metaData.getColumnType(column)) {
                    case 2:
                    case 4:
                        stringBuilder.append(resultSet.getInt(column));
                        break;
                    case 12:
                        stringBuilder.append(resultSet.getString(column));
                        break;
                    case 93:
                        stringBuilder.append(resultSet.getTimestamp(column).toString());
                        break;

                    default:
                        stringBuilder.append("column type - ").append(metaData.getColumnType(column));
                        break;
                }
                System.out.println(stringBuilder.toString());
            }
            System.out.println();
        }

    }
}
