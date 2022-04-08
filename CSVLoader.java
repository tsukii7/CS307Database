package database;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Properties;

public class CSVLoader {
    private static final int BATCH_SIZE = 500;
    private static URL propertyURL = CSVLoader.class
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
            stmt = con.prepareStatement("insert into contract_info values("+s+")");
//            head_stmt, sale_stmt, prod_model_stmt, prod_class_stmt, supply_stmt,
//            client_stmt, order_stmt, model_class_stmt, cont_stmt;
//            stmts = new PreparedStatement[9];
//            stmts[0] = con.prepareStatement("insert into header values(?,?)");
//            stmts[1] = con.prepareStatement("insert into sales values(?,?,?,?,?)");
//            stmts[2] = con.prepareStatement("insert into product_model values(?,?)");
//            stmts[3] = con.prepareStatement("insert into product_class values(?,?)");
//            stmts[4] = con.prepareStatement("insert into supply values(?,?)");
//            stmts[5] = con.prepareStatement("insert into client values(?,?,?,?)");
//            stmts[6] = con.prepareStatement("insert into \"order\" values(?,?,?,?,?,?)");
//            stmts[7] = con.prepareStatement("insert into model_class values(?,?)");
//            stmts[8] = con.prepareStatement("insert into contract values(?,?,?)");
//            stmt = con.prepareStatement("insert into students(studentid,name)"
//                    + " values(?,?)");
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
                if(stmt != null)
                stmt.close();
//                for (int i = 0; i < stmts.length; i++) {
//                    if (stmts[i] != null)
//                        stmts[i].close();
//                }
                con.close();
                con = null;
            } catch (Exception e) {
                // Forget about it
            }
        }
    }

    //    private static void loadData(String studentid, String name)
//            throws SQLException {
//        if (con != null) {
//            stmt.setString(1, studentid);
//            stmt.setString(2, name);
//            stmt.addBatch();
//        }
//    }

//    String contract_num(0), client_etpr(1), supply_center(2), country(3), city(4), industry(5), prod_code(6), prod_name(7), prod_model(8),
//            cont_date(11), esti_deli_date(12), lodge_date(13), director(14), salesman(15), sale_num(16), sale_gender(17), sale_phone(19);
//    int sale_age(18), unit_price(9), quantity(10);
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
                stmt.setString(i,parts[i-1]);
            }
            stmt.setInt(10,unit_price);
            stmt.setInt(11,quantity);
            stmt.setDate(12,cont_date);
            stmt.setDate(13,esti_deli_date);
            stmt.setDate(14,lodge_date);
            for (int i = 15; i < 19; i++) {
                stmt.setString(i,parts[i-1]);
            }
            stmt.setInt(19,sale_age);
            stmt.setString(20, parts[19]);

//            stmts[0].setString(1, parts[0]);
//            stmts[0].setDate(2, cont_date);
//            stmts[1].setString(1, parts[15]);
//            stmts[1].setString(2, parts[15]);
//            stmts[1].setString(3, parts[19]);
//            stmts[1].setInt(4, sale_age);
//            stmts[1].setString(5, parts[17]);
//            stmts[2].setString(1, parts[8]);
//            stmts[2].setInt(2, unit_price);
//            stmts[3].setString(1, parts[6]);
//            stmts[3].setString(2, parts[7]);
//            stmts[4].setString(1, parts[2]);
//            stmts[4].setString(2, parts[14]);
//            stmts[5].setString(1, parts[1]);
//            stmts[5].setString(2, parts[3]);
//            stmts[5].setString(3, parts[4]);
//            stmts[5].setString(4, parts[5]);
//            stmts[6].setString(1, parts[0]);
//            stmts[6].setString(2, parts[8]);
//            stmts[6].setString(3, parts[16]);
//            stmts[6].setInt(4, quantity);
//            stmts[6].setDate(5, esti_deli_date);
//            stmts[6].setDate(6, lodge_date);
//            stmts[7].setString(1, parts[6]);
//            stmts[7].setString(2, parts[8]);
//            stmts[8].setString(1, parts[0]);
//            stmts[8].setString(2, parts[2]);
//            stmts[8].setString(3, parts[1]);
//            stmt.setString(1, studentid);
//            stmt.setString(2, name);
                stmt.addBatch();
//            for (int i = 0; i < stmts.length; i++) {
//                stmts[i].addBatch();
//            }
        }
    }

    public static void main(String[] args) {
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

//        if (propertyURL == null) {
//           System.err.println("No configuration file (loader.cnf) found");
//           System.exit(1);
//        }
        Properties defprop = new Properties();
        defprop.put("host", "localhost");
        defprop.put("user", "checker");
        defprop.put("password", "123456");
        defprop.put("database", "sustc");
        Properties prop = new Properties(defprop);
//        try (BufferedReader conf
//                = new BufferedReader(new FileReader(propertyURL.getPath()))) {
//          prop.load(conf);
//        } catch (IOException e) {
//           // Ignore
//           System.err.println("No configuration file (loader.cnf) found");
//        }
        try (BufferedReader infile
                     = new BufferedReader(new FileReader(fileName))) {
            long start;
            long end;
            String line;
            String[] parts;
            String[] titles;
            int cnt = 0;
            // Empty target table
//            openDB(prop.getProperty("host"), prop.getProperty("database"),
//                    prop.getProperty("user"), prop.getProperty("password"));
//            Statement stmt0;
//            if (con != null) {
//                stmt0 = con.createStatement();
//                stmt0.execute("truncate table students");
//                stmt0.close();
//            }
//            closeDB();
            //
            start = System.currentTimeMillis();
            openDB(prop.getProperty("host"), prop.getProperty("database"),
                    prop.getProperty("user"), prop.getProperty("password"));
            if ((line = infile.readLine()) != null) {
                titles = line.split(",");
            }
            while ((line = infile.readLine()) != null) {
                parts = line.split(",");
                if (parts.length > 1) {
//                    studentid = parts[0].replace(",", "");
//                    name = parts[1];
                    loadData(parts);
                    cnt++;
                    if (cnt % BATCH_SIZE == 0) {
                        stmt.executeBatch();
                        stmt.clearBatch();
//                        for (int i = 0; i < stmts.length; i++) {
//                            stmts[i].executeBatch();
//                            stmts[i].clearBatch();
//                        }
                    }
                }
            }
            if (cnt % BATCH_SIZE != 0) {
                stmt.executeBatch();
                stmt.clearBatch();
//                for (int i = 0; i < stmts.length; i++) {
//                    stmts[i].executeBatch();
//                    stmts[i].clearBatch();
//                }
            }
            con.commit();
//            for (int i = 0; i < stmts.length; i++) {
//                stmts[i].close();
//            }
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
//                for (int i = 0; i < stmts.length; i++) {
//                    stmts[i].close();
//                }
                stmt.close();

            } catch (Exception e2) {
            }
            closeDB();
            System.exit(1);
        } catch (IOException e) {
            System.err.println("Fatal error: " + e.getMessage());
            try {
                con.rollback();
//                for (int i = 0; i < stmts.length; i++) {
//                    stmts[i].close();
//                }
                stmt.close();
            } catch (Exception e2) {
            }
            closeDB();
            System.exit(1);
        }
        closeDB();
    }
}

