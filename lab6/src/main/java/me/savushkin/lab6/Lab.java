package me.savushkin.lab6;

import me.savushkin.common.LabHelper;
import oracle.jdbc.pool.OracleConnectionPoolDataSource;
import oracle.jdbc.pool.OracleDataSource;
import oracle.jdbc.rowset.OracleJDBCRowSet;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;
import javax.sql.PooledConnection;
import java.io.File;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Hashtable;
import java.util.logging.Logger;

public class Lab {

    public static void main(String[] args) {
        try {

            OracleJDBCRowSet ojrs = new OracleJDBCRowSet();
            ojrs.setUrl("jdbc:oracle:thin:@localhost:1521:orbis");
            ojrs.setUsername("stud");
            ojrs.setPassword("stud");
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
