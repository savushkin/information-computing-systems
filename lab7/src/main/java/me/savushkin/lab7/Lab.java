package me.savushkin.lab7;

import oracle.jdbc.pool.OracleDataSource;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import java.io.File;
import java.sql.*;
import java.util.Hashtable;

public class Lab {
    // 182190
    // var. 0

    // Получить список студентов своей группы и вывести на экран номер группы,
    // фамилию, имя, отчество, дату рождения и место рождения, используя указанные
    // для каждого варианта методы и интерфейсы.

    // 0. Создать объект типа DataSource и сохранить его, используя JNDI;
    // запросить сохраненный объект и получить, используя его, соединение с базой данных;
    // выполнить запрос методом execute(), используя объект типа Statement;
    // при выводе результатов использовать метод next().

    private final static String ORACLE_URL = "jdbc:oracle:thin:@localhost:1521:orbis";
    private final static String ORACLE_DRIVER = "oracle.jdbc.driver.OracleDriver";
    private final static String ORACLE_USER = "";
    private final static String ORACLE_PASS = "";

    private final static String DATA_SOURCE_NAME = "database";
    private final static String CONTEXT_DIR_NAME = "JNDI";
    private final static String CONTEXT_PROVIDER_URL = String.format("file:%s", CONTEXT_DIR_NAME);
    private final static String CONTEXT_INITIAL_CONTEXT_FACTORY = "com.sun.jndi.fscontext.RefFSContextFactory";

    private final static String GROUP_NUM = "4110";

    public static void main(String[] args) {
        File JNDIDir = new File(CONTEXT_DIR_NAME);
        if (!JNDIDir.exists())
            JNDIDir.mkdir();

        // Создать объект типа DataSource и сохранить его, используя JNDI;
        InitialContext context = null;
        OracleDataSource dataSource = null;
        PreparedStatement statement = null;
        Connection connection = null;
        try {
            dataSource = new OracleDataSource();
            dataSource.setURL(ORACLE_URL);
            dataSource.setUser(ORACLE_USER);
            dataSource.setPassword(ORACLE_PASS);

            Hashtable hashtable = new Hashtable();
            hashtable.put(Context.INITIAL_CONTEXT_FACTORY,
                    CONTEXT_INITIAL_CONTEXT_FACTORY);
            hashtable.put(Context.PROVIDER_URL,
                    CONTEXT_PROVIDER_URL);
            context = new InitialContext(hashtable);
            context.rebind(DATA_SOURCE_NAME, dataSource);
            context.close();
            dataSource = null;

            // запросить сохраненный объект и получить, используя его, соединение с базой данных;
            hashtable = new Hashtable();
            hashtable.put(Context.INITIAL_CONTEXT_FACTORY,
                    CONTEXT_INITIAL_CONTEXT_FACTORY);
            hashtable.put(Context.PROVIDER_URL,
                    CONTEXT_PROVIDER_URL);
            context = new InitialContext(hashtable);
            dataSource = (OracleDataSource) context.lookup(DATA_SOURCE_NAME);
            connection = dataSource.getConnection();

            // выполнить запрос методом execute(), используя объект типа Statement;
            statement = connection.prepareStatement(
                    "SELECT У.ГРУППА, Л.ФАМИЛИЯ, Л.ИМЯ, Л.ОТЧЕСТВО, Л.ДАТА_РОЖДЕНИЯ, Л.МЕСТО_РОЖДЕНИЯ " +
                            "FROM Н_ЛЮДИ Л JOIN Н_УЧЕНИКИ У ON Л.ИД = У.ЧЛВК_ИД " +
                            "WHERE У.ГРУППА = ?");
            statement.setString(1, GROUP_NUM);
            if ( statement.execute() ) {
                ResultSet resultSet = statement.getResultSet();
                ResultSetMetaData metaData = resultSet.getMetaData();

                // при выводе результатов использовать метод next().
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

                resultSet.close();
            }
        } catch (SQLException | NamingException e) {
            e.printStackTrace();
        } finally {
            try {
                if ( statement != null )
                    statement.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }

            try {
                if ( connection != null )
                    connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }

            try {
                if ( context != null )
                    context.close();
            } catch (NamingException e) {
                e.printStackTrace();
            }
        }
    }
}