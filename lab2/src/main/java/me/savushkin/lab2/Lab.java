package me.savushkin.lab2;

import me.savushkin.common.LabHelper;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.sql.*;
import java.util.Arrays;
import java.util.logging.Logger;

public class Lab {
    private static String ORACLE_URL = "jdbc:oracle:thin:@localhost:1521:orbis";
    private static String ORACLE_DRIVER = "oracle.jdbc.driver.OracleDriver";
    private static String ORACLE_USER = "";
    private static String ORACLE_PASS = "";
    private static String POSTGRES_URL = "jdbc:postgresql://pg:5432/ucheb";
    private static String POSTGRES_DRIVER = "org.postgresql.Driver";
    private static String POSTGRES_USER = "";
    private static String POSTGRES_PASS = "";

    static Logger log = Logger.getLogger(Lab.class.getName());

    public static void main(String[] args) {
        if (args.length > 0) {

            Connection connection = null;
            try {
                LabHelper.registration(ORACLE_DRIVER);
                //LabHelper.registration(POSTGRES_DRIVER);

                connection = LabHelper.connection(ORACLE_URL, ORACLE_USER, ORACLE_PASS);
                //connection = LabHelper.connection(POSTGRES_URL, POSTGRES_USER, POSTGRES_PASS);

                Statement statement = connection.createStatement();

                switch (args[0]) {
                    case "task1": {
                        createTables(statement);
                        insertData(statement);
                        break;
                    }
                    case "task2": {
                        boolean executeResult;
                        executeResult = statement.execute(
                                "INSERT INTO номера_телефонов VALUES(121018,'9999999',{d '2000-4-20'},1)");
                        processExecute(statement, executeResult);
                        executeResult = statement.execute(
                                "SELECT фамилия, имя, номер_телефона, дата_регистрации, оператор "
                                        + "FROM н_люди л, номера_телефонов т, операторы_связи о "
                                        + "WHERE л.ид = т.ид_л AND т.ид_ос = о.ид");
                        processExecute(statement, executeResult);
                        break;
                    }
                    case "task3": {
                        createTables(statement);
                        batchInsert(statement);
                        break;
                    }
                    case "task4": {
                        Savepoint savepoint = null;
                        try {
                            connection.setAutoCommit(false);
                            if (!connection.getAutoCommit())
                                System.out.println("Auto-Commit отменен...");
                            statement.executeUpdate(
                                    "INSERT INTO операторы_связи VALUES(5,'TELE2')");
                            savepoint = connection.setSavepoint();
                            statement.executeUpdate(
                                    "INSERT INTO номера_телефонов VALUES(130777,'2223322',{d '2004-2-3'},5)");
                            connection.commit();
                            connection.setAutoCommit(true);
                        } catch (SQLException e) {
                            System.out.println("SQLException сообщение: " + e.getMessage());
                            System.out.println("Начнем откат изменений...");
                            if (savepoint != null)
                                connection.rollback(savepoint);
                            else
                                connection.rollback();
                            System.out.println("Откат изменений закончен...");
                        }
                        break;
                    }
                    case "task5": {
                        createTables(statement);
                        insertData(connection);
                        System.out.println("Создание базы данных закончено...");
                        break;
                    }
                    case "task6": {
                        createReadmeTable(statement);
                        File file = new File("/etc/passwd");
                        long fileLength = file.length();
                        FileInputStream fileInputStream = new FileInputStream(file);
                        PreparedStatement preparedStatement = connection.prepareStatement(
                                "INSERT INTO readme VALUES(?,?)");
                        preparedStatement.setInt(1,1);
                        preparedStatement.setAsciiStream(2, fileInputStream, (int)fileLength);
                        preparedStatement.execute();
                        fileInputStream.close();
                        ResultSet resultSet = statement.executeQuery(
                                "SELECT * FROM readme WHERE id = 1");
                        resultSet.next();
                        InputStream inputStream = resultSet.getAsciiStream(2);
                        int c;
                        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
                        while ((c = inputStream.read()) != -1)
                            byteArrayOutputStream.write(c);
                        System.out.println(byteArrayOutputStream.toString());
                        break;
                    }
                    case "task7": {
                        createProc(statement);
                        executeProc(connection);
                        break;
                    }
                    default:
                        System.err.println("Undefined argument");
                        break;
                }
                statement.close();
            } catch (SQLException | ClassNotFoundException e) {
                e.printStackTrace();
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

            System.out.println("Программа завершена.");
        }
    }

    // task 1
    private static void createTables(Statement statement) throws SQLException {
        String operatorsql = "CREATE TABLE операторы_связи (ид NUMERIC(3) CONSTRAINT ПК_ОС PRIMARY KEY, оператор VARCHAR(20))";
        String telefonsql = "CREATE TABLE номера_телефонов (ид_л NUMERIC(9) CONSTRAINT ВК_Л REFERENCES н_люди(ид), номер_телефона VARCHAR(20), дата_регистрации DATE, ид_ос NUMERIC(3) CONSTRAINT ВК_ОС REFERENCES операторы_связи(ид))";
        try {
            statement.executeUpdate("DROP TABLE операторы_связи CASCADE CONSTRAINTS");
        } catch (SQLException se) {
            //Игнорировать ошибку удаления таблицы
            if(se.getErrorCode()==942) {
                String msg = se.getMessage();
                System.out.println("Ошибка при удалении таблицы: "+msg);
            }
        }
        //Создание таблицы операторы_связи
        if(statement.executeUpdate(operatorsql)==0)
            System.out.println("Таблица операторы_связи создана...");
        try {
            statement.executeUpdate("DROP TABLE номера_телефонов");
        } catch (SQLException se) {
            //Игнорировать ошибку удаления таблицы
            if(se.getErrorCode()==942) {
                String msg = se.getMessage();
                System.out.println("Ошибка при удалении таблицы: "+msg);
            }
        }
        //Создание таблицы номера_телефонов
        if(statement.executeUpdate(telefonsql)==0)
            System.out.println("Таблица номера_телефонов создана...");
    }

    // task 1
    private static void insertData(Statement statement) throws SQLException {

        //Загрузка данных в таблицу операторы_связи
        statement.executeUpdate("INSERT INTO операторы_связи VALUES(1,'Мегафон')");
        statement.executeUpdate("INSERT INTO операторы_связи VALUES(2,'МТС')");
        statement.executeUpdate("INSERT INTO операторы_связи VALUES(3,'Би Лайн')");
        statement.executeUpdate("INSERT INTO операторы_связи VALUES(4,'SkyLink')");

        //Загрузка данных в таблицу номера_телефонов
        statement.executeUpdate("INSERT INTO номера_телефонов VALUES(125704,'9363636',{d '2000-9-15'},1)");
        statement.executeUpdate("INSERT INTO номера_телефонов VALUES(125704,'2313131',{d '2001-2-25'},2)");
        statement.executeUpdate("INSERT INTO номера_телефонов VALUES(125704,'1151515',{d '2002-6-17'},4)");
        statement.executeUpdate("INSERT INTO номера_телефонов VALUES(120848,'4454545',{d '2003-7-30'},3)");
        statement.executeUpdate("INSERT INTO номера_телефонов VALUES(120848,'1161616',{d '2004-1-16'},4)");
        System.out.println("Загрузка данных закончена...");
    }

    // task 2
    private static void  processExecute(Statement st, boolean executeResult) throws SQLException {
        do {
            ResultSet resultSet = st.getResultSet();
            if (resultSet != null)
                LabHelper.printResultSet(resultSet);
            else
                System.out.println(String.format("Количество созданных или изменённых строк - %d\n", st.getUpdateCount()));
        } while (!(!st.getMoreResults() && st.getUpdateCount() == -1));
    }

    // task 3
    private static void batchInsert(Statement statement) throws SQLException {

        //Загрузка данных в таблицу операторы_связи
        statement.addBatch("INSERT INTO операторы_связи VALUES(1,'Мегафон')");
        statement.addBatch("INSERT INTO операторы_связи VALUES(2,'МТС')");
        statement.addBatch("INSERT INTO операторы_связи VALUES(3,'Би Лайн')");
        statement.addBatch("INSERT INTO операторы_связи VALUES(4,'SkyLink')");

        //Загрузка данных в таблицу номера_телефонов
        statement.addBatch("INSERT INTO номера_телефонов VALUES(125704,'9363636',{d '2000-9-15'},1)");
        statement.addBatch("INSERT INTO номера_телефонов VALUES(125704,'2313131',{d '2001-2-25'},2)");
        statement.addBatch("INSERT INTO номера_телефонов VALUES(125704,'1151515',{d '2002-6-17'},4)");
        statement.addBatch("INSERT INTO номера_телефонов VALUES(120848,'4454545',{d '2003-7-30'},3)");
        statement.addBatch("INSERT INTO номера_телефонов VALUES(120848,'1161616',{d '2004-1-16'},4)");
        System.out.println("Пакет загрузки данных подготовлен...");
        //statement.clearBatch();
        Arrays.stream(statement.executeBatch()).forEach(i -> System.out.print(i+"\t"));
        System.out.println("\n"+"Загрузка данных закончена...");
        statement.clearBatch();
    }

    // task 5
    private static void insertData(Connection connection) throws SQLException {
        //Загрузка данных в таблицу операторы_связи
        int count;
        PreparedStatement preparedStatement = connection.prepareStatement(
                "INSERT INTO операторы_связи VALUES(?,?)");
        preparedStatement.setInt(1,1);
        preparedStatement.setString(2,"Мегафон");
        if(preparedStatement.executeUpdate() == 1)
            System.out.println("Запись \"Мегафон\" добавлена");
        preparedStatement.setInt(1,2);
        preparedStatement.setString(2,"МТС");
        if(preparedStatement.executeUpdate() == 1)
            System.out.println("Запись \"МТС\" добавлена");
        preparedStatement.setInt(1,3);
        preparedStatement.setString(2,"Би Лайн");
        if(preparedStatement.executeUpdate() == 1)
            System.out.println("Запись \"Би Лайн\" добавлена");
        preparedStatement.setInt(1,4);
        preparedStatement.setString(2,"SkyLink");
        if(preparedStatement.executeUpdate() == 1)
            System.out.println("Запись \"SkyLink\" добавлена");
        preparedStatement.close();
        //
        //Загрузка данных в таблицу номера_телефонов
        preparedStatement = connection.prepareStatement(
                "INSERT INTO номера_телефонов VALUES(?,?,?,?)");
        connection.setAutoCommit(false);
        //
        preparedStatement.setInt(1,125704);
        preparedStatement.setString(2,"9363636");
        preparedStatement.setDate(3,Date.valueOf("2000-9-15"));
        preparedStatement.setInt(4,1);
        preparedStatement.addBatch();

        preparedStatement.setInt(1,125704);
        preparedStatement.setString(2,"2313131");
        preparedStatement.setDate(3,Date.valueOf("2001-2-25"));
        preparedStatement.setInt(4,2);
        preparedStatement.addBatch();

        preparedStatement.setInt(1,125704);
        preparedStatement.setString(2,"1151515");
        preparedStatement.setDate(3,Date.valueOf("2002-6-17"));
        preparedStatement.setInt(4,4);
        preparedStatement.addBatch();

        preparedStatement.setInt(1,120848);
        preparedStatement.setString(2,"4454545");
        preparedStatement.setDate(3,Date.valueOf("2003-7-30"));
        preparedStatement.setInt(4,3);
        preparedStatement.addBatch();

        preparedStatement.setInt(1,120848);
        preparedStatement.setString(2,"1161616");
        preparedStatement.setDate(3,Date.valueOf("2004-1-16"));
        preparedStatement.setInt(4,4);
        preparedStatement.addBatch();

        Arrays.stream(preparedStatement.executeBatch()).forEach(i -> System.out.print(i+"\t"));
        preparedStatement.clearBatch();
        preparedStatement.close();
        connection.commit();
        System.out.println("\n"+"Загрузка данных закончена...");
        connection.setAutoCommit(true);
    }

    // task 6
    public static void createReadmeTable(Statement statement) throws Exception {
        try {
            statement.executeUpdate("DROP TABLE readme");
        } catch (SQLException se) {
            //Игнорировать ошибку удаления таблицы
            if(se.getErrorCode()==942) {
                String msg = se.getMessage();
                System.out.println("Ошибка при удалении таблицы: "+msg);
            }
        }
        //Создание таблицы
        if(statement.executeUpdate("CREATE TABLE readme (id INTEGER, data LONG)")==0)
            System.out.println("Таблица readme создана...");
    }

    // task 7
    public static void createProc(Statement statement) throws SQLException {
        String sql = "CREATE OR REPLACE PROCEDURE get_fio "
                +"(id IN NUMBER, fio OUT VARCHAR) AS "
                +"BEGIN "
                +"SELECT человек(id, 'И', 9) INTO fio FROM DUAL; "
                +"END;";
        try {
            statement.executeUpdate("DROP PROCEDURE get_fio");
        } catch (SQLException se) {
            //Игнорировать ошибку удаления процедуры
            if(se.getErrorCode()==4043) {
                String msg = se.getMessage();
                System.out.println("Ошибка при удалении процедуры: "+msg);
            }
        }
        //Создание процедуры
        if(statement.executeUpdate(sql)==0)
            System.out.println("Процедура get_fio создана...");
    }

    // task 7
    public static void executeProc(Connection connection) throws SQLException {
        String sql = "{call get_fio(?,?)}";
        CallableStatement cst = connection.prepareCall(sql);
        cst.setInt(1,121018);
        cst.registerOutParameter(2,Types.VARCHAR);
        cst.execute();
        System.out.println("Результат запроса: "+cst.getString(2));
    }
}
