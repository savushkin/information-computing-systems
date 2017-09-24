package me.savushkin.lab1;

import me.savushkin.common.LabHelper;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Logger;

public class Lab {
    static Logger log = Logger.getLogger(Lab.class.getName());

    public static void main(String[] args) {
        Connection connection = null;
        try {
            //LabHelper.registration("org.postgresql.Driver");
            LabHelper.registration("oracle.jdbc.driver.OracleDriver");

            //connection = LabHelper.connection(
            //        "jdbc:postgresql://pg:5432/ucheb","", "");
            connection = LabHelper.connection(
                    "jdbc:oracle:thin:@localhost:1521:orbis","", "");

            List<String> queryList = Arrays.asList(
                    "SELECT * FROM Н_ЦИКЛЫ_ДИСЦИПЛИН",
                    "SELECT АББРЕВИАТУРА, НАИМЕНОВАНИЕ FROM Н_ЦИКЛЫ_ДИСЦИПЛИН",
                    "SELECT НАИМЕНОВАНИЕ FROM Н_КВАЛИФИКАЦИИ",
                    "SELECT DISTINCT ИМЯ FROM Н_ЛЮДИ",
                    "SELECT DISTINCT ПРИЗНАК FROM Н_УЧЕНИКИ");
            Statement statement = connection.createStatement();
            queryList.forEach(query -> {
                try {
                    ResultSet resultSet = statement.executeQuery(query);
                    System.out.println(query);
                    LabHelper.printResultSet(resultSet);
                    resultSet.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });

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
}
