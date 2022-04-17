package database;

import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Properties;

public class LoadWithCopy {
    private static URL propertyURL = LoadWithCSVpackage.class
            .getResource("/loader.cnf");
    private static Connection con = null;
//    private static PreparedStatement stmt = null;
    private static boolean verbose = false;

    private static void openDB(String host, String dbname,
                               String user, String pwd) {
        try {
            //
            Class.forName("org.postgresql.Driver");
        } catch (Exception e) {
            System.err.println("Cannot find the Postgres driver. Check CLASSPATH.");
            System.exit(1);
        }
        String url = "jdbc:postgresql://" + host + "/" + dbname;
        Properties props = new Properties();
        props.setProperty("user", user);
        props.setProperty("password", pwd);
        try {
            con = DriverManager.getConnection(url, props);
            if (verbose) {
                System.out.println("Successfully connected to the database "
                        + dbname + " as " + user);
            }
            con.setAutoCommit(false);
        } catch (SQLException e) {
            System.err.println("Database connection failed");
            System.err.println(e.getMessage());
            System.exit(1);
        }
    }

    private static void closeDB() {
        if (con != null) {
            try {
//                if (stmt != null)
//                    stmt.close();
                con.close();
                con = null;
            } catch (Exception e) {
                // Forget about it
            }
        }
    }

    public static void main(String[] args) {
        long starTime = System.currentTimeMillis();
        Properties defprop = new Properties();
        defprop.put("host", "localhost");
        defprop.put("user", "checker");
        defprop.put("password", "123456");
        defprop.put("database", "sustc");
        defprop.put("encoding","UNICODE");
        Properties prop = new Properties(defprop);
        try {
            openDB(prop.getProperty("host"), prop.getProperty("database"),
                    prop.getProperty("user"), prop.getProperty("password"));
            con.commit();
            CopyManager copyManager = new CopyManager((BaseConnection) con);
            String[] filenames = new String[9];
            String[] tables = new String[]{"header", "sales", "product_model", "product_class", "supply", "client", "order", "model_class", "contract"};
            for (int i = 0; i < 9; i++) {
                filenames[i] = "C:\\Users\\21414\\Documents\\Curriculum\\database\\project01\\CS307Database\\sustc_tables\\" + tables[i] + ".csv";
            }
            FileInputStream file = null;
            long start;
            long end;
            int[] cnt = new int[]{5000,990,961,325,7,167,50000,961,5000};
            for (int i = 0; i < 9; i++) {

                start = System.currentTimeMillis();
                file = new FileInputStream(filenames[i]);
                String copy = "COPY " + tables[i]+ " FROM STDIN DELIMITER AS ','";
                copyManager.copyIn(copy,file);

                end = System.currentTimeMillis();
                System.out.println(cnt[i] + " records successfully loaded");
                System.out.println("Loading speed : "
                        + (cnt[i] * 1000) / (end - start)
                        + " records/s");
            }
            closeDB();
        } catch (SQLException | FileNotFoundException se) {
            System.err.println("SQL error: " + se.getMessage());
            try {
                con.rollback();
//                stmt.close();
            } catch (Exception ignored) {
            }
            closeDB();
            System.exit(1);
        } catch (IOException e) {
            e.printStackTrace();
        }
        closeDB();
        long endTime = System.currentTimeMillis();
        System.out.println("程序运行时间：" + 1.0 * (endTime - starTime) / 1000 + "s");
    }
}

