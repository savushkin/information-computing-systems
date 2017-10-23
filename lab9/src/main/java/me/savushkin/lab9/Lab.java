package me.savushkin.lab9;

import oracle.jdbc.rowset.OracleCachedRowSet;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.sql.*;
import java.util.Arrays;
import java.util.List;

public class Lab {
    // 1. Восстановить объект типа CachedRowSet из файла, сохраненного при выполнении задания №8 в домашнем каталоге студента, ID которого совпадает с номером варианта, выданного преподавателем.
    // 2. Вывести на экран данные полученного объекта типа CachedRowSet в виде таблицы с названиями столбцов и указанием типа данных каждого столбца, для чего необходимо получить объект типа ResultSetMetaData и воспользоваться его методами.
    // 3. Создать в базе данных таблицу с соответствующим количеством столбцов и заполнить ее данными объекта типа CachedRowSet. Типы данных объекта CachedRowSet и столбцов таблицы должны совпадать.
    // 4. Обновить данные в таблице и выдать их используя тот же самый объект CachedRowSet в виде таблицы с названиями столбцов и указанием типа данных каждого столбца, для чего необходимо получить объект типа ResultSetMetaData и воспользоваться его методами.
    private final static String ORACLE_URL = "jdbc:oracle:thin:@localhost:1521:orbis";
    private final static String ORACLE_DRIVER = "oracle.jdbc.driver.OracleDriver";
    private final static String ORACLE_USER = "";
    private final static String ORACLE_PASS = "";

    private final static String FILENAME = "S182190_1";


    public static void main(String[] args) {
        // 1. Восстановить объект типа CachedRowSet из файла, сохраненного при выполнении задания №8 в домашнем каталоге студента, ID которого совпадает с номером варианта, выданного преподавателем.
        OracleCachedRowSet oracleCachedRowSet;
        try (FileInputStream fileInputStream = new FileInputStream(FILENAME)) {
            try (ObjectInputStream objectInputStream = new ObjectInputStream(fileInputStream)) {
                oracleCachedRowSet = (OracleCachedRowSet) objectInputStream.readObject();
                System.out.println(FILENAME);

                // 2. Вывести на экран данные полученного объекта типа CachedRowSet в виде таблицы с названиями столбцов и указанием типа данных каждого столбца, для чего необходимо получить объект типа ResultSetMetaData и воспользоваться его методами.
                print(oracleCachedRowSet);

                // 3. Создать в базе данных таблицу с соответствующим количеством столбцов и заполнить ее данными объекта типа CachedRowSet. Типы данных объекта CachedRowSet и столбцов таблицы должны совпадать.
                try (Connection connection = DriverManager.getConnection(ORACLE_URL, ORACLE_USER, ORACLE_PASS)) {
                    try (Statement statement = connection.createStatement()) {
                        StringBuilder query = new StringBuilder("CREATE TABLE ");
                        query.append(FILENAME);
                        query.append(" (");

                        ResultSetMetaData metaData = oracleCachedRowSet.getMetaData();
                        for (int column = 1; column <= metaData.getColumnCount(); column++) {
                            query.append(metaData.getColumnName(column));
                            query.append(" ");
                            query.append(metaData.getColumnTypeName(column));
                            query.append("(");
                            switch (metaData.getColumnType(column)) {
                                case 2:
                                case 4:
                                    int size = metaData.getColumnDisplaySize(column);
                                    query.append(size > 38? 38: size);
                                    query.append(", ");
                                    query.append("2");
                                    break;
                                default:
                                    query.append(metaData.getColumnDisplaySize(column));
                                    break;
                            }
                            query.append(")");
                            if (column != metaData.getColumnCount())
                                query.append(", ");
                        }

                        query.append(")");
                        try {
                            System.out.printf("DROP TABLE %s CASCADE CONSTRAINTS%n", FILENAME);
                            statement.executeUpdate(String.format("DROP TABLE %s CASCADE CONSTRAINTS", FILENAME));
                        } catch (SQLException e) {
                            System.out.println(e.getMessage());
                        }
                        System.out.println(query.toString());
                        statement.executeUpdate(query.toString());
                    }
                }

                System.out.println();
                try (OracleCachedRowSet dbCachedRowSet = new OracleCachedRowSet()) {
                    dbCachedRowSet.setUrl(ORACLE_URL);
                    dbCachedRowSet.setUsername(ORACLE_USER);
                    dbCachedRowSet.setPassword(ORACLE_PASS);
                    ResultSetMetaData metaData = oracleCachedRowSet.getMetaData();
                    StringBuilder query = new StringBuilder("SELECT ");
                    for (int column = 1; column <= metaData.getColumnCount(); column++) {
                        query.append(metaData.getColumnName(column));
                        if (column != metaData.getColumnCount())
                            query.append(", ");
                    }
                    query.append(" FROM ");
                    query.append(FILENAME);

                    System.out.println(query.toString());
                    dbCachedRowSet.setCommand(query.toString());
                    dbCachedRowSet.execute();
                    dbCachedRowSet.setReadOnly(false);
                    print(dbCachedRowSet);
                    oracleCachedRowSet.beforeFirst();
                    dbCachedRowSet.beforeFirst();
                    while (oracleCachedRowSet.next()) {
                        dbCachedRowSet.moveToInsertRow();
                        for (int column = 1; column <= metaData.getColumnCount(); column++)
                            switch (metaData.getColumnType(column)) {
                                case 2:
                                case 4:
                                    dbCachedRowSet.updateDouble(column, oracleCachedRowSet.getDouble(column));
                                    break;
                                case 12:
                                    dbCachedRowSet.updateString(column, oracleCachedRowSet.getString(column));
                                    break;
                                default:
                                    break;
                            }
                        dbCachedRowSet.insertRow();
                    }
                    dbCachedRowSet.acceptChanges();
                    dbCachedRowSet.execute();
                    print(dbCachedRowSet);

                    // 4. Обновить данные в таблице и выдать их используя тот же самый объект CachedRowSet в виде таблицы с названиями столбцов и указанием типа данных каждого столбца, для чего необходимо получить объект типа ResultSetMetaData и воспользоваться его методами.
                    dbCachedRowSet.first();
                    dbCachedRowSet.updateString("ФАМИЛИЯ", "Савушкин");
                    dbCachedRowSet.updateString("ИМЯ", "Иван");
                    dbCachedRowSet.updateString("ОТЧЕСТВО", "Евгеньевич");
                    dbCachedRowSet.updateDouble("СРЕДНЯЯ_ОЦЕНКА", 5.0);
                    dbCachedRowSet.updateRow();
                    dbCachedRowSet.acceptChanges();
                    dbCachedRowSet.execute();
                    print(dbCachedRowSet);
                }
            }
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void print(ResultSet resultSet) throws SQLException {
        ResultSetMetaData metaData = resultSet.getMetaData();

        while (resultSet.next()) {
            int columnsCount = metaData.getColumnCount();
            for (int column = 1; column <= columnsCount; column++) {
                StringBuilder stringBuilder = new StringBuilder();
                stringBuilder.append(metaData.getColumnName(column)).append(": ");
                stringBuilder.append("(" + metaData.getColumnTypeName(column) + ") ");
                switch (metaData.getColumnType(column)) {
                    case 2:
                    case 4:
                        stringBuilder.append(resultSet.getDouble(column));
                        break;
                    case 12:
                        stringBuilder.append(resultSet.getString(column));
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
