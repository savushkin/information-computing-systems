package ru.makhnovets.lab9;

import oracle.jdbc.rowset.OracleCachedRowSet;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.sql.*;

public class S182119 {
    // 1. Восстановить объект типа CachedRowSet из файла, сохраненного при выполнении задания №8 в домашнем каталоге студента, ID которого совпадает с номером варианта, выданного преподавателем.
    // 2. Вывести на экран данные полученного объекта типа CachedRowSet в виде таблицы с названиями столбцов и указанием типа данных каждого столбца, для чего необходимо получить объект типа ResultSetMetaData и воспользоваться его методами.
    // 3. Создать в базе данных таблицу с соответствующим количеством столбцов и заполнить ее данными объекта типа CachedRowSet. Типы данных объекта CachedRowSet и столбцов таблицы должны совпадать.
    // 4. Обновить данные в таблице и выдать их используя тот же самый объект CachedRowSet в виде таблицы с названиями столбцов и указанием типа данных каждого столбца, для чего необходимо получить объект типа ResultSetMetaData и воспользоваться его методами.
    private final static String ORACLE_URL = "jdbc:oracle:thin:@localhost:1521:orbis";
    private final static String ORACLE_DRIVER = "oracle.jdbc.driver.OracleDriver";
    private final static String ORACLE_USER = "";
    private final static String ORACLE_PASS = "";

    private final static String FILENAME = "S182119";


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

                        try {
                            System.out.printf("DROP TABLE %s CASCADE CONSTRAINTS%n", FILENAME);
                            statement.executeUpdate(String.format("DROP TABLE %s CASCADE CONSTRAINTS", FILENAME));
                        } catch (SQLException e) {
                            System.out.println(e.getMessage());
                        }
                        String createQuery = buildCreateQuery(oracleCachedRowSet.getMetaData());
                        System.out.println(createQuery);
                        statement.executeUpdate(createQuery);
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
                                case 93:
                                    dbCachedRowSet.updateDate(column, oracleCachedRowSet.getDate(column));
                                    break;
                                case 2:
                                case 4:
                                    dbCachedRowSet.updateBigDecimal(column, oracleCachedRowSet.getBigDecimal(column));
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
                    dbCachedRowSet.updateString("ФАМИЛИЯ", "Махновец");
                    dbCachedRowSet.updateString("ИМЯ", "Валерия");
                    dbCachedRowSet.updateString("ОТЧЕСТВО", "Алексеевна");
                    dbCachedRowSet.updateRow();
                    dbCachedRowSet.acceptChanges();
                    dbCachedRowSet.execute();
                    print(dbCachedRowSet);
                }
            }
        } catch (ClassNotFoundException | SQLException | IOException e) {
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
                    case 93:
                        stringBuilder.append(resultSet.getDate(column));
                        break;
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

    private static String buildCreateQuery(ResultSetMetaData metaData) throws SQLException {
        StringBuilder query = new StringBuilder("CREATE TABLE ");
        query.append(FILENAME);
        query.append(" (");

        for (int column = 1; column <= metaData.getColumnCount(); column++) {
            query.append(metaData.getColumnName(column));
            query.append(" ");
            query.append(metaData.getColumnTypeName(column));
            switch (metaData.getColumnType(column)) {
                case 93:
                    break;
                case 2:
                case 4:
                    query.append("(");
                    int size = metaData.getColumnDisplaySize(column);
                    query.append(size > 38? 38: size);
                    query.append(")");
                    break;
                default:
                    query.append("(");
                    query.append(metaData.getColumnDisplaySize(column));
                    query.append(")");
                    break;
            }
            if (column != metaData.getColumnCount())
                query.append(", ");
        }

        query.append(")");
        return query.toString();
    }
}



