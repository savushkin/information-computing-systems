package ru.makhnovets.lab7;

import oracle.jdbc.rowset.OracleCachedRowSet;
import oracle.jdbc.rowset.OracleJDBCRowSet;

import java.io.*;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

public class S182119 {
    /*
    182119
    var. 9

    Получить список студентов своей группы и вывести на экран номер группы, фамилию, имя, отчество,
    дату рождения и место рождения, используя указанные для каждого варианта методы и интерфейсы.

    9. Создать объект типа RowSet и сохранить его в файле, используя ObjectOutput; восстановить сохраненный
    объект и получить результаты запроса; при выводе результатов использовать метод previous().
    */

    private static String ORACLE_DRIVER = "oracle.jdbc.driver.OracleDriver";
    private static String ORACLE_USER = "";
    private static String ORACLE_PASS = "";
    private static String ORACLE_URL = "jdbc:oracle:thin:@localhost:1521:orbis";

    private final static String GROUP_NUM = "5110";
    private final static String DATA_FILE = GROUP_NUM + ".data";

    public static void main(String[] args) {
        // Создать объект типа RowSet и сохранить его в файле, используя ObjectOutput;
        try (OracleCachedRowSet rowSet = new OracleCachedRowSet()) {
            rowSet.setUrl(ORACLE_URL);
            rowSet.setUsername(ORACLE_USER);
            rowSet.setPassword(ORACLE_PASS);
            rowSet.setCommand("SELECT У.ГРУППА, Л.ФАМИЛИЯ, Л.ИМЯ, Л.ОТЧЕСТВО, Л.ДАТА_РОЖДЕНИЯ, Л.МЕСТО_РОЖДЕНИЯ FROM Н_ЛЮДИ Л JOIN Н_УЧЕНИКИ У ON Л.ИД = У.ЧЛВК_ИД WHERE У.ГРУППА = ?");
            rowSet.setString(1, GROUP_NUM);
            System.out.println("Запрос...");
            rowSet.execute();
            print(rowSet);

            System.out.println("Сохранение...");
            try (FileOutputStream fileOutputStream = new FileOutputStream(DATA_FILE)) {
                try (ObjectOutputStream objectOutputStream = new ObjectOutputStream(fileOutputStream)) {
                    objectOutputStream.writeObject(rowSet);
                }
            }
        } catch (SQLException | IOException e) {
            e.printStackTrace();
        }

        // восстановить сохраненный объект и получить результаты запроса;
        try (FileInputStream fileInputStream = new FileInputStream(DATA_FILE)) {
            try (ObjectInputStream objectOutputStream = new ObjectInputStream(fileInputStream)) {
                System.out.println("Чтение...");
                try (OracleCachedRowSet rowSet = (OracleCachedRowSet) objectOutputStream.readObject()) {
                    print(rowSet);
                }
            }
        } catch (ClassNotFoundException | SQLException | IOException e) {
            e.printStackTrace();
        }
    }

    private static void print(ResultSet resultSet) throws SQLException {
        // при выводе результатов использовать метод next().
        ResultSetMetaData metaData = resultSet.getMetaData();

        resultSet.afterLast();
        while (resultSet.previous()) {
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



