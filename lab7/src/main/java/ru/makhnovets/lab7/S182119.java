package ru.makhnovets.lab7;

import oracle.jdbc.rowset.OracleJDBCRowSet;

import java.sql.SQLException;

public class S182119 {
    /*
    На контрольном занятии необходимо создать java-класс, последовательно демонстрирующий
        все шаги организации доступа к базе данных, рассмотренные на первом занятии,
        включая пошаговую обработку ошибок.
    Название класса должно совпадать с идентификатором студента, выполняющего задание.
    Класс и исходный файл с расширением .java должны находится в домашнем каталоге студента,
        выполняющего задание.
    Каждый шаг выполнения задания необходимо сопровождать выводом соответствующий результатов, демонстрирующих правильность его выполнения. Все шаги должны быть снабжены подробными комментариями.
    Время выполнения контрольного задания – 1 час 20 минут.

    9. Создать объект типа RowSet и сохранить его в файле, используя ObjectOutput;
     восстановить сохраненный объект и получить результаты запроса;
      при выводе результатов использовать метод previous().
    */
    private static String ORACLE_DRIVER = "oracle.jdbc.driver.OracleDriver";
    private static String ORACLE_USER = "s182119";
    private static String ORACLE_PASS = "fax573";
    private static String ORACLE_URL = String.format(
            "jdbc:oracle:thin:%s/%s@localhost:1521:orbis",
            ORACLE_USER,
            ORACLE_PASS);

    public static void main(String[] args) {
        try {

            OracleJDBCRowSet ojrs = new OracleJDBCRowSet();
            ojrs.setUrl("jdbc:oracle:thin:@localhost:1521:orbis");
            ojrs.setUsername("s182119");
            ojrs.setPassword("fax573");
            ojrs.setCommand("SELECT count(*) FROM н_люди");
            ojrs.execute();
            while (ojrs.next()) {
                System.out.println("Total count of row is: " + ojrs.getInt(1));
            }

        } catch (SQLException se) {
            se.printStackTrace();
        }
        System.out.println("Goodbye!");
    }
}



