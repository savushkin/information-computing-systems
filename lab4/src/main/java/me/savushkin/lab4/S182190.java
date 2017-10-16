package me.savushkin.lab4;


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.CallableStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;

import static java.lang.String.format;

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
    private static String ORACLE_USER = "";
    private static String ORACLE_PASS = "";

    private static String TABLE_NAME = "APPLICATION_LIST";
    private static String UPDATE_PROCEDURE_NAME = "UPDATE_TABLE_ENTITY";

    private static String QUERY_LIST = format(
            "drop table %1$s cascade constraints\n" +
                    //таблица со списком приложений, их версий, компаний разработчиков и электронных адресов
                    "create table %1$s (id NUMBER, application_name VARCHAR(50), version VARCHAR(50), release NUMBER(1,0), company VARCHAR(50), email VARCHAR(50))\n" +
                    "insert into %1$s (id, application_name, version, release, company, email) values (1, 'Hatity', '0.3.1', 0, 'Kwimbee', 'ndoige0@columbia.edu')\n" +
                    "insert into %1$s (id, application_name, version, release, company, email) values (2, 'Tempsoft', '2.3', 0, 'Mymm', 'bchiverton1@blog.com')\n" +
                    "insert into %1$s (id, application_name, version, release, company, email) values (3, 'Matsoft', '0.13', 0, 'Fatz', 'lcarruth2@behance.net')\n" +
                    "insert into %1$s (id, application_name, version, release, company, email) values (4, 'Stringtough', '0.15', 0, 'Skibox', 'ereinhardt3@state.gov')\n" +
                    "insert into %1$s (id, application_name, version, release, company, email) values (5, 'Transcof', '0.9.2', 0, 'Livetube', 'mstrut4@1und1.de')\n" +
                    "insert into %1$s (id, application_name, version, release, company, email) values (6, 'Tempsoft', '0.59', 1, 'Abata', 'aseawell5@people.com.cn')\n" +
                    "insert into %1$s (id, application_name, version, release, company, email) values (7, 'Matsoft', '0.9.2', 0, 'Avavee', 'bporter6@theguardian.com')\n" +
                    "insert into %1$s (id, application_name, version, release, company, email) values (8, 'It', '0.64', 0, 'Layo', 'djozwik7@wisc.edu')\n" +
                    "insert into %1$s (id, application_name, version, release, company, email) values (9, 'Bitwolf', '4.1', 0, 'Vinder', 'ymasdon8@google.ru')\n" +
                    "insert into %1$s (id, application_name, version, release, company, email) values (10, 'Aerified', '0.2.6', 0, 'Jayo', 'cbarrable9@vimeo.com')\n" +
                    "insert into %1$s (id, application_name, version, release, company, email) values (11, 'Tresom', '0.1.7', 1, 'Pixoboo', 'darnia@ft.com')\n" +
                    "insert into %1$s (id, application_name, version, release, company, email) values (12, 'Subin', '0.9.2', 0, 'Thoughtstorm', 'kmcavaddyb@simplemachines.org')\n" +
                    "insert into %1$s (id, application_name, version, release, company, email) values (13, 'Cardify', '7.9.6', 0, 'Jetwire', 'oschneidauc@earthlink.net')\n" +
                    "insert into %1$s (id, application_name, version, release, company, email) values (14, 'Wrapsafe', '0.12', 1, 'Vipe', 'ldoughtond@dmoz.org')\n" +
                    "insert into %1$s (id, application_name, version, release, company, email) values (15, 'Overhold', '0.2.6', 0, 'Thoughtstorm', 'tborrowse@engadget.com')\n" +
                    "insert into %1$s (id, application_name, version, release, company, email) values (16, 'Bitwolf', '0.7.8', 0, 'Skyba', 'jdearnf@domainmarket.com')\n" +
                    "insert into %1$s (id, application_name, version, release, company, email) values (17, 'Konklux', '9.6.7', 1, 'Yotz', 'oservisg@adobe.com')\n" +
                    "insert into %1$s (id, application_name, version, release, company, email) values (18, 'Alphazap', '1.6.0', 1, 'Feedspan', 'sskethh@google.it')\n" +
                    "insert into %1$s (id, application_name, version, release, company, email) values (19, 'Konklux', '2.72', 0, 'DabZ', 'agooderei@surveymonkey.com')\n" +
                    "insert into %1$s (id, application_name, version, release, company, email) values (20, 'Tampflex', '0.95', 0, 'Wordware', 'rfashionj@freewebs.com')\n" +
                    "insert into %1$s (id, application_name, version, release, company, email) values (21, 'Bigtax', '4.7.1', 0, 'Rooxo', 'tivettk@bbb.org')\n" +
                    //хранимая процедура для обновления данных
                    "create or replace procedure %2$s (id_entity in INT) as begin update %1$s set version='42.255.34', release=1 where id=id_entity; end;",
            TABLE_NAME,
            UPDATE_PROCEDURE_NAME);


    public static void main(String[] args) {
        Connection connection = null;
        try {
            Class.forName(ORACLE_DRIVER);
            connection = DriverManager.getConnection(ORACLE_URL, ORACLE_USER, ORACLE_PASS);
            Statement statement = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);

            for (String query : QUERY_LIST.split("\n")) {
                try {
                    System.out.println(query);
                    statement.executeUpdate(query);
                } catch (SQLException se) {
                    System.out.println(se.getMessage());
                    if(se.getErrorCode()==942)
                        System.out.printf("Ошибка при удалении таблицы: %s%n", se.getMessage());
                    else
                        se.printStackTrace();
                }
            }

            int count = printResultSet(statement.executeQuery(format("select * from %s", TABLE_NAME)));
            System.out.println(count);
            int randomId = (int) (Math.random() * count) + 1;

            System.out.printf("update %s set version='42.255.34', release=1 where id=%s%n", TABLE_NAME, randomId);
            CallableStatement callableStatement = connection.prepareCall(format("{call %s(?)}", UPDATE_PROCEDURE_NAME));
            callableStatement.setInt(1, randomId);
            callableStatement.execute();

            printResultSet(statement.executeQuery(format("select * from %s", TABLE_NAME)));

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

    public static int printResultSet(ResultSet resultSet) throws SQLException {
        ResultSetMetaData metaData = resultSet.getMetaData();

        while(resultSet.next()) {
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

        resultSet.last();
        return resultSet.getRow();
    }
}
