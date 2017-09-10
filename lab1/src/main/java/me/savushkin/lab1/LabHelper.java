package me.savushkin.lab1;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
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
}
