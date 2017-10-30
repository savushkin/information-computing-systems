package ru.makhnovets.lab8;

import oracle.jdbc.rowset.OracleCachedRowSet;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.sql.*;

public class S182119 {
    /*
    На контрольном занятии необходимо создать java-класс, включающий в себя следующие обязательные действия:
    1. Подготовить предложение SELECT для выполнения запроса в соответствии с выданным преподавателем вариантом.
    2. Выполнить запрос и получить результат в объект типа ResultSet.
    3. Создать объект типа CachedRowSet и заполнить его результатами запроса, находящимися в объекте типа ResultSet, методом populate().
    4. Сохранить объект типа CachedRowSet в файле, имя которого должно совпадать с идентификатором студента, выполняющего задание, и находиться в корне его домашнего каталога.
    Название класса должно совпадать с идентификатором студента, выполняющего задание. Класс и исходный файл с расширением .java должны находится в корне домашнего каталога студента, выполняющего задание.
    Каждый шаг выполнения задания необходимо сопровождать выводом соответствующий результатов, демонстрирующих правильность его выполнения. Все шаги должны быть снабжены подробными комментариями.
    На защите необходимо быть готовым продемонстрировать альтернативную реализацию любого, указанного преподавателем, шага выполненного контрольного задания; уметь восстанавливать сохраненный объект типа CachedRowSet и выводить на экран содержащиеся в нем данные.
    Время выполнения контрольного задания – 1 час 20 минут.

    37. Преобразовать предыдущий запрос, используя для организации подзапросов предикат EXISTS (проверка на существование).
        36. Преобразовать запрос п. 32 так, чтобы во фразе FROM осталось соединение только тех таблиц, столбцы которых входят в списки фраз SELECT и ORDER BY. Остальные таблицы, данные из которых нужны для отбора нужных строк результата, необходимо разместить во фразе WHERE, не используя их соединений. Для организации подзапросов использовать предикат IN (проверка на принадлежность).
        32. Получить список студентов, зачисленных первого сентября позапрошлого учебного года на первый курс очной формы обучения специальности 220100. В результат включить:
        1) номер группы,
        2) номер, фамилию, имя и отчество студента,
        3) номер и состояние пункта приказа, признак, характеризующий состояние студента,
            дату конца действия этого пункта,
        Результат упорядочить по номеру группы и фамилии.

    */
    private static String ORACLE_DRIVER = "oracle.jdbc.driver.OracleDriver";
    private static String ORACLE_USER = "s182119";
    private static String ORACLE_PASS = "fax573";
    private static String ORACLE_URL = "jdbc:oracle:thin:@localhost:1521:orbis";

    private final static String NUMBER = "220100";

    private final static String QUERY = "select н_ученики.группа, н_люди.ид, н_люди.фамилия, н_люди.имя, н_люди.отчество, н_ученики.п_пркок_ид, н_ученики.состояние, н_ученики.признак, н_ученики.конец from н_люди join н_ученики on н_люди.ид = н_ученики.члвк_ид where exists(select н_планы.ид from н_планы where exists(select н_формы_обучения.ид from н_формы_обучения where н_формы_обучения.наименование in ('Очная') and н_планы.фо_ид = н_формы_обучения.ид) and exists(select н_направления_специал.ид from н_направления_специал where exists(select н_напр_спец.ид from н_напр_спец where н_направления_специал.нс_ид = н_напр_спец.ид and н_напр_спец.код_напрспец in (?)) and н_планы.напс_ид = н_направления_специал.ид) and н_планы.курс='1' and to_char(н_ученики.начало,'YYYY') in (to_char(SYSDATE,'YYYY')-17) and to_char(н_ученики.начало,'DD.MM') in('01.09') and н_ученики.план_ид = н_планы.ид) ORDER BY н_ученики.группа, н_люди.фамилия";

    public static void main(String[] args) {
        try {
            Class.forName(ORACLE_DRIVER);
            try (Connection connection = DriverManager.getConnection(ORACLE_URL, ORACLE_USER, ORACLE_PASS)) {
                try (PreparedStatement statement = connection.prepareStatement(QUERY)) {
                    statement.setString(1, NUMBER);
                    try (ResultSet resultSet = statement.executeQuery()) {
                        OracleCachedRowSet oracleCachedRowSet = new OracleCachedRowSet();
                        oracleCachedRowSet.populate(resultSet);

                        try (ObjectOutputStream objectOutputStream =
                                     new ObjectOutputStream(
                                             new FileOutputStream("S182119"))) {
                            objectOutputStream.writeObject(oracleCachedRowSet);
                            objectOutputStream.close();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                }
            }
        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        }
    }
}



