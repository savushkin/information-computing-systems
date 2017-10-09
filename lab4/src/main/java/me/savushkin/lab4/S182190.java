package me.savushkin.lab4;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.logging.Logger;

public class S182190 {
    //12823
    //1. Указать импорт каждого используемого класса и интерфейса.
    //2. Class.forName
    //3,4,8. DriverManager.getConnection(String url, String user, String pass)
    //2. Statement, executeUpdate()
    //3. TYPE_SCROLL_INSENSITIVE, CONCUR_READ_ONLY, ResultSetMetaData
    //Для всех вариантов метод close().

    private static String ORACLE_URL = "jdbc:oracle:thin:@localhost:1521:orbis";
    private static String ORACLE_DRIVER = "oracle.jdbc.driver.OracleDriver";
    private static String ORACLE_USER = "s182190";
    private static String ORACLE_PASS = "tny395";

    private static String TABLE_NAME = "mishka";

    public static void main(String[] args) {
            Connection connection = null;
            try {
                Class.forName(ORACLE_DRIVER);
                connection = DriverManager.getConnection(ORACLE_URL, ORACLE_USER, ORACLE_PASS);
                Statement statement = connection.createStatement();

                try {
                    statement.executeUpdate(
                            String.format("DROP TABLE %s CASCADE CONSTRAINTS", TABLE_NAME));
                } catch (SQLException se) {
                    if(se.getErrorCode()==942)
                        System.out.printf("Ошибка при удалении таблицы: %s%n", se.getMessage());
                }

                statement.executeUpdate(
                        String.format("CREATE TABLE %s (ид_л NUMERIC(9), номер_телефона VARCHAR(20), дата_регистрации DATE, ид_ос NUMERIC(3))", TABLE_NAME));

                statement.executeUpdate("INSERT INTO номера_телефонов VALUES(125704, '9363636', {d '2000-9-15'}, 1)");
                statement.executeUpdate("INSERT INTO номера_телефонов VALUES(125704, '2313131', {d '2001-2-25'}, 2)");
                statement.executeUpdate("INSERT INTO номера_телефонов VALUES(125704, '1151515', {d '2002-6-17'}, 4)");
                statement.executeUpdate("INSERT INTO номера_телефонов VALUES(120848, '4454545', {d '2003-7-30'}, 3)");
                statement.executeUpdate("INSERT INTO номера_телефонов VALUES(120848, '1161616', {d '2004-1-16'}, 4)");

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
