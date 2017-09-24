package me.savushkin.lab2;

import me.savushkin.common.LabHelper;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.logging.Logger;

public class Lab {
    static Logger log = Logger.getLogger(Lab.class.getName());

    public static void main(String[] args) {
        Connection connection = null;
        try {
            //LabHelper.registration("org.postgresql.Driver");
            LabHelper.registration("oracle.jdbc.driver.OracleDriver");

            //connection = LabHelper.connection(
            //        "jdbc:postgresql://pg:5432/ucheb","", "");
            connection = LabHelper.connection(
                    "jdbc:oracle:thin:@localhost:1521:orbis","", "");

            Statement statement = connection.createStatement();
            createTables(statement);
            insertData(statement);
            statement.close();

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

}
