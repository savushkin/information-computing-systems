package me.savushkin.lab2;

import me.savushkin.common.LabHelper;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.sql.*;
import java.util.logging.Logger;

public class Lab {
    private static String ORACLE_URL = "jdbc:oracle:thin:@localhost:1521:orbis";
    private static String ORACLE_DRIVER = "oracle.jdbc.driver.OracleDriver";
    private static String ORACLE_USER = "s182119";
    private static String ORACLE_PASS = "ujt532";
    private static String POSTGRES_URL = "jdbc:postgresql://pg:5432/ucheb";
    private static String POSTGRES_DRIVER = "org.postgresql.Driver";
    private static String POSTGRES_USER = "";
    private static String POSTGRES_PASS = "";

    static Logger log = Logger.getLogger(Lab.class.getName());
    static Connection conn = null;

    public static void main(String[] args) {
        if (args.length > 0)
            switch (args[0]) {
                case "task1": {
                    Connection connection = null;
                    try {
                        //LabHelper.registration(POSTGRES_DRIVER);
                        LabHelper.registration(ORACLE_DRIVER);

                        //connection = LabHelper.connection(POSTGRES_URL, POSTGRES_USER, POSTGRES_PASS);
                        connection = LabHelper.connection(ORACLE_URL, ORACLE_USER, ORACLE_PASS);

                        Statement statement = connection.createStatement();
                        createTables(statement);
                        insertData(statement);
                        statement.close();

                    } catch (Exception e) {
                        e.printStackTrace();
                    } finally {
                        try {
                            if (connection != null)
                                connection.close();
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                    break;
                }
                case "task2": {
                    Connection connection = null;
                    Statement st = null;
                    ResultSet rs = null;
                    boolean executeResult;
                    try {
                        //LabHelper.registration(POSTGRES_DRIVER);
                        LabHelper.registration(ORACLE_DRIVER);

                        //connection = LabHelper.connection(POSTGRES_URL, POSTGRES_USER, POSTGRES_PASS);
                        connection = LabHelper.connection(ORACLE_URL, ORACLE_USER, ORACLE_PASS);

                        st = connection.createStatement();

                        String sql = "INSERT INTO номера_телефонов VALUES(121018,'9999999',{d '2000-4-20'},1)";
                        executeResult = st.execute(sql);
                        processExecute(st, executeResult);

                        sql = "SELECT фамилия, имя, номер_телефона, дата_регистрации, оператор "
                                +"FROM н_люди л, номера_телефонов т, операторы_связи о "
                                +"WHERE л.ид = т.ид_л AND т.ид_ос = о.ид";
                        executeResult = st.execute(sql);
                        processExecute(st, executeResult);

                        System.out.println("Программа завершена.");

                    } catch (Exception e) {
                        e.printStackTrace();
                    } finally {
                        try {
                            if (connection != null)
                                connection.close();
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                    break;
                }
                case "task3":{
                    //definition of variables
                    Connection connection = null;
                    Statement st = null;
                    ResultSet rs = null;
                    boolean executeResult;
                    Savepoint svpt = null;
                    try {
                        //driver registration
                        LabHelper.registration(ORACLE_DRIVER);
                        //connection creation
                        connection = LabHelper.connection(ORACLE_URL, ORACLE_USER, ORACLE_PASS);
                        st = connection.createStatement();
                        connection.setAutoCommit(false);
                        if (!connection.getAutoCommit()) System.out.println("Auto-Commit отменен...");
                        String sql = "INSERT INTO операторы_связи VALUES(5,'TELE2')";
                        st.executeUpdate(sql);
                        svpt = connection.setSavepoint("Point1");
                        sql = "INSERT INTO номера_телефонов VALUES(130777,'2223322',{d '2004-2-3'},5)";
                        st.executeUpdate(sql);
                        connection.commit();
                    }catch (SQLException se) {
                        System.out.println("SQLException сообщение: "+se.getMessage());
                        System.out.println("Начнем откат изменений...");
                        try {
                            //conn.rollback();
                            connection.rollback(svpt);
                        } catch (SQLException rse) {
                            rse.printStackTrace();
                        }
                        System.out.println("Откат изменений закончен...");
                    } catch (Exception e) {
                        e.printStackTrace();
                    } finally {
                        try {
                            if(connection!=null) connection.close();
                        } catch (SQLException se) {
                            se.printStackTrace();
                        }
                    }
                    System.out.println("Программа завершена.");
                }
                case "task4": {
                    try{
                        //driver registration
                        LabHelper.registration(ORACLE_DRIVER);
                        //connection creation
                        conn = LabHelper.connection(ORACLE_URL, ORACLE_USER, ORACLE_PASS); //Регистрация драйвера, и создание соединения
                        //
                        createTables();
                        insertData();
                        //
                        System.out.println("Создание базы данных закончено...");
                    } catch (SQLException se) {
                        se.printStackTrace();
                    } catch (Exception e) {
                        e.printStackTrace();
                    } finally {
                        try {
                            if(conn!=null) conn.close();
                        } catch (SQLException se) {
                            se.printStackTrace();
                        }
                    }
                    System.out.println("Программа завершена.");
                }
                case "task5":{
                    Connection connection = null;
                    Statement statement = null;
                    PreparedStatement prst = null;
                    ResultSet rs = null;
                    try {
                        //Регистрация драйвера, и создание соединения
                        //
                        statement = conn.createStatement();
                        createReadmeTable(statement);
                        //input path to file
                        File f = new File("/etc/passwd");
                        long fileLength = f.length();
                        FileInputStream fis = new FileInputStream(f);
                        //
                        String sql = "INSERT INTO readme VALUES(?,?)";
                        prst = conn.prepareStatement(sql);
                        prst.setInt(1,1);
                        prst.setAsciiStream(2,fis,(int)fileLength);
                        prst.execute();
                        //
                        fis.close();
                        //
                        sql = "SELECT * FROM readme WHERE id = 1";
                        rs = statement.executeQuery(sql);
                        if(rs.next()) {
                            InputStream is = rs.getAsciiStream(2);
                            int c;
                            ByteArrayOutputStream baos = new ByteArrayOutputStream();
                            while ((c = is.read()) != -1) baos.write(c);
                            System.out.println(baos.toString());
                        }
                    } catch (SQLException se) {
                        se.printStackTrace();
                    } catch (Exception e) {
                        e.printStackTrace();
                    } finally {
                        try {
                            if(conn!=null) conn.close();
                        } catch (SQLException se) {
                            se.printStackTrace();
                        }
                    }
                    System.out.println("Программа завершена.");
                }
                default:
                    System.err.println("Undefined argument");
                    break;
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
        int[] count = statement.executeBatch();
        for(int i=0; i<count.length; i++) System.out.print(count[i]+"\t");
        statement.clearBatch();
    }
    //task 4
    public static void createTables() throws SQLException {
        Statement statement = conn.createStatement();
        //Программный код создания таблиц базы данных (см. пример 1-1)
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

    public static void insertData() throws SQLException {
        //Загрузка данных в таблицу операторы_связи
        int count;
        String sql = "INSERT INTO операторы_связи VALUES(?,?)";
        PreparedStatement prst = conn.prepareStatement(sql);
        //
        prst.setInt(1,1); prst.setString(2,"Мегафон"); count = prst.executeUpdate();
        if(count==1)
            System.out.println("Запись \"Мегафон\" добавлена");
        prst.setInt(1,2); prst.setString(2,"МТС"); count = prst.executeUpdate();
        if(count==1)
            System.out.println("Запись \"МТС\" добавлена");
        prst.setInt(1,3); prst.setString(2,"Би Лайн"); count = prst.executeUpdate();
        if(count==1)
            System.out.println("Запись \"Би Лайн\" добавлена");
        prst.setInt(1,4); prst.setString(2,"SkyLink"); count = prst.executeUpdate();
        if(count==1)
            System.out.println("Запись \"SkyLink\" добавлена");
        prst.close();
        //
        //Загрузка данных в таблицу номера_телефонов
        sql = "INSERT INTO номера_телефонов VALUES(?,?,?,?)";
        prst = conn.prepareStatement(sql);
        conn.setAutoCommit(false);
        //
        prst.setInt(1,125704); prst.setString(2,"9363636"); prst.setDate(3,Date.valueOf("2000-9-15")); prst.setInt(4,1); prst.addBatch();
        prst.setInt(1,125704); prst.setString(2,"2313131"); prst.setDate(3,Date.valueOf("2001-2-25")); prst.setInt(4,2); prst.addBatch();
        prst.setInt(1,125704); prst.setString(2,"1151515"); prst.setDate(3,Date.valueOf("2002-6-17")); prst.setInt(4,4); prst.addBatch();
        prst.setInt(1,120848); prst.setString(2,"4454545"); prst.setDate(3,Date.valueOf("2003-7-30")); prst.setInt(4,3); prst.addBatch();
        prst.setInt(1,120848); prst.setString(2,"1161616"); prst.setDate(3,Date.valueOf("2004-1-16")); prst.setInt(4,4); prst.addBatch();
        int[] countb = prst.executeBatch();
        for(int i=0; i<countb.length; i++) System.out.print(countb[i]+"\t");
        prst.clearBatch();
        prst.close();
        conn.commit();
        System.out.println("\n"+"Загрузка данных закончена...");
    }
    //task 5
    public static void createReadmeTable(Statement st) throws Exception {
        String sql = "CREATE TABLE readme (id INTEGER, data LONG)";
        try {
            st.executeUpdate("DROP TABLE readme");
        } catch (SQLException se) {
            //Игнорировать ошибку удаления таблицы
            if(se.getErrorCode()==942) {
                String msg = se.getMessage();
                System.out.println("Ошибка при удалении таблицы: "+msg);
            }
        }
        //Создание таблицы
        if(st.executeUpdate(sql)==0)
            System.out.println("Таблица readme создана...");
    }
}
