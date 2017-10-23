package me.savushkin.lab8;

import oracle.jdbc.rowset.OracleCachedRowSet;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.sql.*;
import java.util.Arrays;
import java.util.List;

public class Lab {
    // 182190
    // var. 23

    // 1. Подготовить предложение SELECT для выполнения запроса в соответствии с выданным преподавателем вариантом.
    // 2. Выполнить запрос и получить результат в объект типа ResultSet.
    // 3. Создать объект типа CachedRowSet и заполнить его результатами запроса, находящимися в объекте типа ResultSet, методом populate().
    // 4. Сохранить объект типа CachedRowSet в файле, имя которого должно совпадать с идентификатором студента, выполняющего задание, и находиться в корне его домашнего каталога.

    // 23. Определите вашу среднюю оценку (естественно, что в расчет должны входить лишь те оценки, которые имеют цифровой эквивалент). Создайте два запроса, в которых средняя оценки определяется с помощью функции среднее значение (функция AVG) и путем деления суммы (функция SUM) оценок на их количество (функция COUNT). В результате необходимо оставить два десятичных знака после запятой. Для округления используется функция ROUND(expr [,m]), возвращающая expr, округленное до m-го десятичного знака; если m опущено, то оно принимается равным 0, а если m < 0, то округляются цифры левее десятичной точки.
    private final static String ORACLE_URL = "jdbc:oracle:thin:@localhost:1521:orbis";
    private final static String ORACLE_DRIVER = "oracle.jdbc.driver.OracleDriver";
    private final static String ORACLE_USER = "";
    private final static String ORACLE_PASS = "";

    private final static List<String> QUERY_MARK = Arrays.asList(
            "SELECT Л.ФАМИЛИЯ, Л.ИМЯ, Л.ОТЧЕСТВО, О.СРЕДНЯЯ_ОЦЕНКА FROM Н_ЛЮДИ Л JOIN (SELECT В.ЧЛВК_ИД, ROUND(AVG(CAST(В.ОЦЕНКА AS DECIMAL(1, 0))), 2) СРЕДНЯЯ_ОЦЕНКА FROM Н_ВЕДОМОСТИ В WHERE В.ОЦЕНКА IN ('2','3','4','5') GROUP BY В.ЧЛВК_ИД) О ON Л.ИД = О.ЧЛВК_ИД WHERE Л.ИД = ?",
            "SELECT Л.ФАМИЛИЯ, Л.ИМЯ, Л.ОТЧЕСТВО, О.СРЕДНЯЯ_ОЦЕНКА FROM Н_ЛЮДИ Л JOIN (SELECT В.ЧЛВК_ИД, ROUND(SUM(CAST(В.ОЦЕНКА AS DECIMAL(1, 0))) / COUNT(В.ОЦЕНКА), 2) СРЕДНЯЯ_ОЦЕНКА FROM Н_ВЕДОМОСТИ В WHERE В.ОЦЕНКА IN ('2','3','4','5') GROUP BY В.ЧЛВК_ИД) О ON Л.ИД = О.ЧЛВК_ИД WHERE Л.ИД = ?"
    );

    private final static String STUDENT_ID = "114092";


    public static void main(String[] argv) {
        try {
            Class.forName(ORACLE_DRIVER);
            try (Connection connection = DriverManager.getConnection(ORACLE_URL, ORACLE_USER, ORACLE_PASS)) {
                int i = 1;
                for (String query: QUERY_MARK)
                    try (PreparedStatement statement = connection.prepareStatement(query);) {
                        statement.setString(1, STUDENT_ID);
                        try (ResultSet resultSet = statement.executeQuery()) {
                            OracleCachedRowSet oracleCachedRowSet = new OracleCachedRowSet();
                            oracleCachedRowSet.populate(resultSet);

                            try (ObjectOutputStream objectOutputStream =
                                         new ObjectOutputStream(
                                                 new FileOutputStream(String.format("S182190_%d", i)))) {
                                objectOutputStream.writeObject(oracleCachedRowSet);
                                objectOutputStream.close();
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                        }
                    }
            }
        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        }
    }

}
