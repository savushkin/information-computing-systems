package me.savushkin.lab1;

import java.sql.*;
import java.util.logging.Logger;

public class Lab {
    static Logger log = Logger.getLogger(Lab.class.getName());

    public static void main(String[] args) {
        Connection connection = null;
        try {
            LabHelper.registration("org.postgresql.Driver");
            //LabHelper.registration(new org.postgresql.Driver());

            connection = LabHelper.connection(
                    "jdbc:postgresql://pg:5432/ucheb","", "");

            String query = null;
            Statement statement = connection.createStatement();

            query = "SELECT * FROM Н_ЦИКЛЫ_ДИСЦИПЛИН";
            ResultSet resultSet = statement.executeQuery(query);
            System.out.println(query);
            Lab.printResult(resultSet);
            resultSet.close();

            query = "SELECT АББРЕВИАТУРА, НАИМЕНОВАНИЕ FROM Н_ЦИКЛЫ_ДИСЦИПЛИН";
            resultSet = statement.executeQuery(query);
            System.out.println(query);
            Lab.printResult(resultSet);
            resultSet.close();

            query = "SELECT НАИМЕНОВАНИЕ FROM Н_КВАЛИФИКАЦИИ";
            resultSet = statement.executeQuery(query);
            System.out.println(query);
            Lab.printResult(resultSet);
            resultSet.close();

            query = "SELECT НАИМЕНОВАНИЕ FROM Н_КВАЛИФИКАЦИИ";
            resultSet = statement.executeQuery(query);
            System.out.println(query);
            Lab.printResult(resultSet);
            resultSet.close();

            query = "SELECT DISTINCT ИМЯ FROM Н_ЛЮДИ";
            resultSet = statement.executeQuery(query);
            System.out.println(query);
            Lab.printResult(resultSet);
            resultSet.close();

            query = "SELECT DISTINCT ПРИЗНАК FROM Н_УЧЕНИКИ";
            resultSet = statement.executeQuery(query);
            System.out.println(query);
            Lab.printResult(resultSet);
            resultSet.close();

            statement.close();

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

    static void printResult(ResultSet resultSet) throws SQLException {
        ResultSetMetaData metaData = resultSet.getMetaData();

        while(resultSet.next()) {
            int columnsCount = metaData.getColumnCount();
            for (int column = 1; column <= columnsCount; column++) {
                StringBuilder stringBuilder = new StringBuilder();
                stringBuilder.append(metaData.getColumnName(column)).append(": ");
                switch (metaData.getColumnType(column)) {
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
