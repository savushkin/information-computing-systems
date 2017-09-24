package me.savushkin.lab2;

import me.savushkin.common.LabHelper;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
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
                default:
                    System.err.println("Undefined argument");
                    break;
            }

    }


    // task 1
    public static void createTables(Statement statement) throws SQLException {
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
    public static void insertData(Statement statement) throws SQLException {

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
    public static void processExecute(Statement st, boolean executeResult) throws SQLException {
        do {
            ResultSet resultSet = st.getResultSet();
            if (resultSet != null)
                LabHelper.printResultSet(resultSet);
            else
                System.out.println(String.format("Количество созданных или изменённых строк - %d\n", st.getUpdateCount()));
        } while (!(!st.getMoreResults() && st.getUpdateCount() == -1));
    }

}
