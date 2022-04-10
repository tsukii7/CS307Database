package database;

import com.csvreader.CsvReader;

import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Properties;

public class LoadWithCSVpackage {
    private static final int BATCH_SIZE = 500;
    private static URL propertyURL = LoadWithCSVpackage.class
            .getResource("/loader.cnf");

    private static Connection con = null;
    private static PreparedStatement stmt = null;
    //    private static PreparedStatement[] stmts;
    //    private static PreparedStatement sale_stmt = null, prod_class_stmt = null, model_class_stmt = null, prod_model_stmt = null,
//            order_stmt = null, head_stmt = null, cont_stmt = null, client_stmt = null, supply_stmt = null;
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
        try {
            String s = "";
            for (int i = 0; i < 19; i++) {
                s += "?,";
            }
            s += "?";
            stmt = con.prepareStatement("insert into contract_info values(" + s + ")");
        } catch (SQLException e) {
            System.err.println("Insert statement failed");
            System.err.println(e.getMessage());
            closeDB();
            System.exit(1);
        }
    }

    private static void closeDB() {
        if (con != null) {
            try {
                if (stmt != null)
                    stmt.close();
                con.close();
                con = null;
            } catch (Exception e) {
                // Forget about it
            }
        }
    }
    private static void loadData(String[] parts)
            throws SQLException {
        if (con != null) {
            java.sql.Date cont_date = new java.sql.Date(new java.util.Date(parts[11].replace("-", "/")).getTime());
            java.sql.Date esti_deli_date = new java.sql.Date(new java.util.Date(parts[12].replace("-", "/")).getTime());
            java.sql.Date lodge_date = null;
            if (parts[13].length() > 0)
                lodge_date = new java.sql.Date(new java.util.Date(parts[13].replace("-", "/")).getTime());
            int sale_age = Integer.parseInt(parts[18]);
            int unit_price = Integer.parseInt(parts[9]);
            int quantity = Integer.parseInt(parts[10]);

            for (int i = 1; i < 10; i++) {
                stmt.setString(i, parts[i - 1]);
            }
            stmt.setInt(10, unit_price);
            stmt.setInt(11, quantity);
            stmt.setDate(12, cont_date);
            stmt.setDate(13, esti_deli_date);
            stmt.setDate(14, lodge_date);
            for (int i = 15; i < 19; i++) {
                stmt.setString(i, parts[i - 1]);
            }
            stmt.setInt(19, sale_age);
            stmt.setString(20, parts[19]);
            stmt.addBatch();
        }
    }

    public static void main(String[] args) {
        long starTime=System.currentTimeMillis();
        String fileName = null;
        boolean verbose = false;

        switch (args.length) {
            case 1:
                fileName = args[0];
                break;
            case 2:
                switch (args[0]) {
                    case "-v":
                        verbose = true;
                        break;
                    default:
                        System.err.println("Usage: java [-v] CSVLoader filename");
                        System.exit(1);
                }
                fileName = args[1];
                break;
            default:
                System.err.println("Usage: java [-v] CSVLoader filename");
                System.exit(1);
        }

        Properties defprop = new Properties();
        defprop.put("host", "localhost");
        defprop.put("user", "checker");
        defprop.put("password", "123456");
        defprop.put("database", "sustc");
        Properties prop = new Properties(defprop);
        try{
            CsvReader reader = new CsvReader(fileName,',', StandardCharsets.UTF_8);
            String[] parts;
            long start;
            long end;
            int cnt = 0;
            start = System.currentTimeMillis();
            openDB(prop.getProperty("host"), prop.getProperty("database"),
                    prop.getProperty("user"), prop.getProperty("password"));
            reader.readHeaders();
            while (reader.readRecord()) {
                parts = reader.getValues();
                if (parts.length > 1) {
                    loadData(parts);
                    cnt++;
                    if (cnt % BATCH_SIZE == 0) {
                        stmt.executeBatch();
                        stmt.clearBatch();
                    }
                }
            }
            if (cnt % BATCH_SIZE != 0) {
                stmt.executeBatch();
                stmt.clearBatch();
            }
            con.commit()
            stmt.close();
            closeDB();
            end = System.currentTimeMillis();
            System.out.println(cnt + " records successfully loaded");
            System.out.println("Loading speed : "
                    + (cnt * 1000) / (end - start)
                    + " records/s");
        } catch (SQLException se) {
            System.err.println("SQL error: " + se.getMessage());
            try {
                con.rollback();
                stmt.close();

            } catch (Exception e2) {
            }
            closeDB();
            System.exit(1);
        } catch (IOException e) {
            System.err.println("Fatal error: " + e.getMessage());
            try {
                con.rollback();
                stmt.close();
            } catch (Exception e2) {
            }
            closeDB();
            System.exit(1);
        }
        closeDB();
        long endTime=System.currentTimeMillis();
        System.out.println("程序运行时间："+1.0*(endTime-starTime)/1000+"s");
    }
}

