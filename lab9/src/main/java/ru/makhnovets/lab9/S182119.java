package ru.makhnovets.lab9;

import oracle.jdbc.rowset.OracleCachedRowSet;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.sql.*;
import java.util.Calendar;
import java.util.Date;

public class S182119 {
    /*
    На зачетном занятии необходимо создать java-класс, включающий в себя следующие обязательные действия:
    1. Восстановить объект типа CachedRowSet из файла, сохраненного при выполнении задания №8 в домашнем каталоге студента, ID которого совпадает с номером варианта, выданного преподавателем.
    2. Вывести на экран данные полученного объекта типа CachedRowSet в виде таблицы с названиями столбцов и указанием типа данных каждого столбца, для чего необходимо получить объект типа ResultSetMetaData и воспользоваться его методами.
    3. Создать в базе данных таблицу с соответствующим количеством столбцов и заполнить ее данными объекта типа CachedRowSet. Типы данных объекта CachedRowSet и столбцов таблицы должны совпадать.
    4. Обновить данные в таблице и выдать их используя тот же самый объект CachedRowSet в виде таблицы с названиями столбцов и указанием типа данных каждого столбца, для чего необходимо получить объект типа ResultSetMetaData и воспользоваться его методами.
    Название класса должно совпадать с идентификатором студента, выполняющего задание. Класс и исходный файл с расширением .java должны находится в корне домашнего каталога студента, выполняющего задание.
    Каждый шаг выполнения задания необходимо сопровождать выводом соответствующий результатов, демонстрирующих правильность его выполнения. Все шаги должны быть снабжены подробными комментариями.
    На защите необходимо знать ВСЁ!!!
    Время выполнения контрольного задания – 1 час 20 минут.
    */
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
                //3. Создать в базе данных таблицу с соответствующим количеством столбцов и заполнить ее данными объекта типа CachedRowSet. Типы данных объекта CachedRowSet и столбцов таблицы должны совпадать.
                Class.forName(ORACLE_DRIVER);
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
                            if (!(metaData.getColumnType(column)==93)) {
                                query.append("(");
                                switch (metaData.getColumnType(column)) {
                                    case 2:
                                    case 4:
                                        int size = metaData.getColumnDisplaySize(column);
                                        query.append(size > 38 ? 38 : size);
                                        break;
                                    default:
                                        query.append(metaData.getColumnDisplaySize(column));
                                        break;
                                }
                                query.append(")");
                            }
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
                                case 93: dbCachedRowSet.updateDate(column, oracleCachedRowSet.getDate(column));
                                default:
                                    break;
                            }
                        dbCachedRowSet.insertRow();
                    }
                    dbCachedRowSet.acceptChanges();
                    dbCachedRowSet.execute();
                    print(dbCachedRowSet);
                    Date date=new Date();
                    // 4. Обновить данные в таблице и выдать их используя тот же самый объект CachedRowSet в виде таблицы с названиями столбцов и указанием типа данных каждого столбца, для чего необходимо получить объект типа ResultSetMetaData и воспользоваться его методами.
                    dbCachedRowSet.first();
                    dbCachedRowSet.updateInt("ГРУППА", 4112);
                    dbCachedRowSet.updateInt("ИД", 182119);
                    dbCachedRowSet.updateString("ФАМИЛИЯ", "Махноввец");
                    dbCachedRowSet.updateString("ИМЯ", "Валерия");
                    dbCachedRowSet.updateString("ОТЧЕСТВО", "Алексеевна");
                    dbCachedRowSet.updateInt("П_ПРКОК_ИД", 112343);
                    dbCachedRowSet.updateString("СОСТОЯНИЕ", "утвержден");
                    dbCachedRowSet.updateString("ПРИЗНАК", "обучен");
                    dbCachedRowSet.updateDate("КОНЕЦ", new java.sql.Date(date.getTime()));
                    dbCachedRowSet.updateRow();
                    dbCachedRowSet.acceptChanges();
                    dbCachedRowSet.execute();
                    print(dbCachedRowSet);
                }
            }


            } catch (IOException e) {
                e.printStackTrace();
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            } catch (SQLException e) {
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
                        stringBuilder.append(resultSet.getInt(column));
                        break;
                    case 12:
                        stringBuilder.append(resultSet.getString(column));
                        break;
                    case 93:
                        stringBuilder.append(resultSet.getDate(column));
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