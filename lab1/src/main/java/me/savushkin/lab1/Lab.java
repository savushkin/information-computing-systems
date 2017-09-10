package me.savushkin.lab1;

import java.sql.*;

public class Lab {
    public static void main(String[] args) {
        Connection connection = null;

        try {
            Class.forName("org.postgresql.Driver");
            connection = DriverManager.getConnection(
                    "jdbc:postgresql://pg:5432/ucheb","", "");
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery("SELECT * FROM Н_ЦИКЛЫ_ДИСЦИПЛИН");
            ResultSetMetaData metaData = resultSet.getMetaData();

            while(resultSet.next()) {
                int columnsCount = metaData.getColumnCount();
                for (int column = 1; column <= columnsCount; column++) {
                    System.out.print(metaData.getColumnName(column) + ": ");
                    switch (metaData.getColumnType(column)) {
                        case 4:
                            System.out.println(resultSet.getInt(column));
                            break;
                        case 12:
                            System.out.println(resultSet.getString(column));
                            break;
                        case 93:
                            System.out.println(resultSet.getTimestamp(column).toString());
                            break;

                        default:
                            System.out.println("column type - " + metaData.getColumnType(column));
                            break;
                    }
                }
                System.out.println();
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (connection != null) {
                    connection.close();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
