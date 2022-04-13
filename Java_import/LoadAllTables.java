package database;

import com.csvreader.CsvReader;

import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.Properties;

public class LoadAllTables {
    private static final int BATCH_SIZE = 500;
    private static URL propertyURL = LoadWithCSVpackage.class
            .getResource("/loader.cnf");

    private static Connection con = null;
    private static PreparedStatement stmt = null;
    private static PreparedStatement[] stmts;
    private static String[] filenames;
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
//            head_stmt, sale_stmt, prod_model_stmt, prod_class_stmt, supply_stmt,
//            client_stmt, order_stmt, model_class_stmt, cont_stmt;
            stmts = new PreparedStatement[9];
            stmts[0] = con.prepareStatement("insert into header values(?,?)");
            stmts[1] = con.prepareStatement("insert into sales values(?,?,?,?,?)");
            stmts[2] = con.prepareStatement("insert into product_model values(?,?)");
            stmts[3] = con.prepareStatement("insert into product_class values(?,?)");
            stmts[4] = con.prepareStatement("insert into supply values(?,?)");
            stmts[5] = con.prepareStatement("insert into client values(?,?,?,?)");
            stmts[6] = con.prepareStatement("insert into \"order\" values(?,?,?,?,?,?)");
            stmts[7] = con.prepareStatement("insert into model_class values(?,?)");
            stmts[8] = con.prepareStatement("insert into contract values(?,?,?)");
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
                for (int i = 0; i < stmts.length; i++) {
                    if (stmts[i] != null)
                        stmts[i].close();
                }
                con.close();
                con = null;
            } catch (Exception e) {
                // Forget about it
            }
        }
    }

    private static void loadhead(String[] parts)
            throws SQLException {
        if (con != null) {
            java.sql.Date cont_date = new java.sql.Date(new java.util.Date(parts[1].replace("-", "/")).getTime());
            stmts[0].setString(1, parts[0]);
            stmts[0].setDate(2, cont_date);
            stmts[0].addBatch();
        }
    }

    private static void loadsale(String[] parts)
            throws SQLException {
        if (con != null) {
            int sale_age = Integer.parseInt(parts[3]);
            for (int i = 0; i < 3; i++) {
                stmts[1].setString(i + 1, parts[i]);

            }
            stmts[1].setInt(4, sale_age);
            stmts[1].setString(5, parts[4]);
            stmts[1].addBatch();
        }
    }

    private static void loadprmo(String[] parts)
            throws SQLException {
        if (con != null) {
            int unit_price = Integer.parseInt(parts[1]);
            stmts[2].setInt(2, unit_price);
            stmts[2].setString(1, parts[0]);
            stmts[2].addBatch();
        }
    }

    private static void loadprcl(String[] parts)
            throws SQLException {
        if (con != null) {
            stmts[3].setString(2, parts[1]);
            stmts[3].setString(1, parts[0]);
            stmts[3].addBatch();
        }
    }

    private static void loadsupp(String[] parts)
            throws SQLException {
        if (con != null) {
            stmts[4].setString(2, parts[1]);
            stmts[4].setString(1, parts[0]);
            stmts[4].addBatch();
        }
    }

    private static void loadclie(String[] parts)
            throws SQLException {
        if (con != null) {
            for (int i = 0; i < 2; i++) {
                stmts[5].setString(i + 1, parts[i]);
            }
            if (parts[2].equals("NULL"))
                stmts[5].setNull(3, Types.VARCHAR);
            else
                stmts[5].setString(3, parts[2]);
            stmts[5].setString(4, parts[3]);
            stmts[5].addBatch();
        }
    }

    private static void loadorde(String[] parts)
            throws SQLException {
        if (con != null) {
            for (int i = 0; i < 3; i++) {
                stmts[6].setString(i + 1, parts[i]);
            }
            int quantity = Integer.parseInt(parts[3]);
            stmts[6].setInt(4, quantity);
            java.sql.Date esti_deli_date = new java.sql.Date(new java.util.Date(parts[4].replace("-", "/")).getTime());
            java.sql.Date lodge_date = null;
            if (parts[5].length() > 0)
                lodge_date = new java.sql.Date(new java.util.Date(parts[5].replace("-", "/")).getTime());
            stmts[6].setDate(5, esti_deli_date);
            stmts[6].setDate(6, lodge_date);
            stmts[6].addBatch();
        }
    }

    private static void loadmocl(String[] parts)
            throws SQLException {
        if (con != null) {
            stmts[7].setString(2, parts[1]);
            stmts[7].setString(1, parts[0]);
            stmts[7].addBatch();
        }
    }

    private static void loadcont(String[] parts)
            throws SQLException {
        if (con != null) {
            for (int i = 0; i < 3; i++) {
                stmts[8].setString(i + 1, parts[i]);
            }
            stmts[8].addBatch();
        }
    }

    public static void main(String[] args) {
        long starTime = System.currentTimeMillis();
        boolean verbose = false;

        filenames = new String[9];
        String[] tables = new String[]{"header", "sales", "product_model", "product_class", "supply", "client", "order", "model_class", "contract"};
        for (int i = 0; i < 9; i++) {
            filenames[i] = "C:\\Users\\21414\\Documents\\Curriculum\\database\\project01\\CS307Database\\sustc_tables\\" + tables[i] + ".csv";
        }
        Properties defprop = new Properties();
        defprop.put("host", "localhost");
        defprop.put("user", "checker");
        defprop.put("password", "123456");
        defprop.put("database", "sustc");
        Properties prop = new Properties(defprop);
        try {
            openDB(prop.getProperty("host"), prop.getProperty("database"),
                    prop.getProperty("user"), prop.getProperty("password"));
            for (int i = 0; i < 9; i++) {
                CsvReader reader = new CsvReader(filenames[i], ',', StandardCharsets.UTF_8);
                String[] parts;
                long start;
                long end;
                int cnt = 0;
                start = System.currentTimeMillis();
                while (reader.readRecord()) {
                    parts = reader.getValues();
                    switch (i) {
                        case 0:
                            loadhead(parts);
                            break;
                        case 1:
                            loadsale(parts);
                            break;
                        case 2:
                            loadprmo(parts);
                            break;
                        case 3:
                            loadprcl(parts);
                            break;
                        case 4:
                            loadsupp(parts);
                            break;
                        case 5:
                            loadclie(parts);
                            break;
                        case 6:
                            loadorde(parts);
                            break;
                        case 7:
                            loadmocl(parts);
                            break;
                        case 8:
                            loadcont(parts);
                            break;
                    }
                    cnt++;
                    if (cnt % BATCH_SIZE == 0) {
                        stmts[i].executeBatch();
                        stmts[i].clearBatch();
                    }

                }
                if (cnt % BATCH_SIZE != 0) {
                    stmts[i].executeBatch();
                    stmts[i].clearBatch();
                }
                con.commit();
                stmts[i].close();
                end = System.currentTimeMillis();
                System.out.println(cnt + " records successfully loaded");
                System.out.println("Loading speed : "
                        + (cnt * 1000) / (end - start)
                        + " records/s");
            }
            closeDB();
        } catch (SQLException se) {
            System.err.println("SQL error: " + se.getMessage());
            try {
                con.rollback();
                for (int i = 0; i < stmts.length; i++) {
                    stmts[i].close();
                }
            } catch (Exception e2) {
            }
            closeDB();
            System.exit(1);
        } catch (IOException e) {
            System.err.println("Fatal error: " + e.getMessage());
            try {
                con.rollback();
                for (int i = 0; i < stmts.length; i++) {
                    stmts[i].close();
                }
            } catch (Exception e2) {
            }
            closeDB();
            System.exit(1);
        }
        closeDB();
        long endTime = System.currentTimeMillis();
        System.out.println("程序运行时间：" + 1.0 * (endTime - starTime) / 1000 + "s");
    }
}

