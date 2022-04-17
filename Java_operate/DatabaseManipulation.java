package Project1.Option;

import java.sql.*;

public class DatabaseManipulation implements DataManipulation {
    private final int BATCH_SIZE = 500;
    private ResultSet resultSet;
    private Connection con = null;
    private String host = "localhost";
    private String dbname = "cs307_2";
    private String user = "checker";
    private String pwd = "123456";
    private String port = "5432";

    @Override
    public void openDatasource() {
        try {
            Class.forName("org.postgresql.Driver");

        } catch (Exception e) {
            System.err.println("Cannot find the PostgreSQL driver. Check CLASSPATH.");
            System.exit(1);
        }

        try {
            String url = "jdbc:postgresql://" + host + ":" + port + "/" + dbname;
            con = DriverManager.getConnection(url, user, pwd);

        } catch (SQLException e) {
            System.err.println("Database connection failed");
            System.err.println(e.getMessage());
            System.exit(1);
        }
    }

    @Override
    public void closeDatasource() {
        if (con != null) {
            try {
                con.close();
                con = null;
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void insertOption(int totalCnt) {
        double totalTime, t1 = System.currentTimeMillis(), t2;
        String sql = "insert into contract_info values " +
                "(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);";
        String[] insertInfo = {
                "",
                "Studio Trigger",
                "Asia",
                "Japan",
                "NULL",
                "Internet",
                "BJS0822",
                "Poster",
                "PosterKIK",
                "50",
                "1000",
                "2013-10-3",
                "2013-11-10",
                "2013-11-11",
                "Steven Edwards",
                "Theo White",
                "12140327",
                "Male",
                "25",
                "13986643179"
        };
        try {
            PreparedStatement stmt = con.prepareStatement(sql);
            stmt.setString(2, insertInfo[1]);
            stmt.setString(3, insertInfo[2]);
            stmt.setString(4, insertInfo[3]);
            stmt.setString(5, insertInfo[4]);
            stmt.setString(6, insertInfo[5]);
            stmt.setString(7, insertInfo[6]);
            stmt.setString(8, insertInfo[7]);
            stmt.setString(9, insertInfo[8]);
            stmt.setInt(10, Integer.parseInt(insertInfo[9]));
            stmt.setInt(11, Integer.parseInt(insertInfo[10]));
            stmt.setDate(12, Date.valueOf(insertInfo[11]));
            stmt.setDate(13, Date.valueOf(insertInfo[12]));
            stmt.setDate(14, Date.valueOf(insertInfo[13]));
            stmt.setString(15, insertInfo[14]);
            stmt.setString(16, insertInfo[15]);
            stmt.setString(17, insertInfo[16]);
            stmt.setString(18, insertInfo[17]);
            stmt.setInt(19, Integer.parseInt(insertInfo[18]));
            stmt.setString(20, insertInfo[19]);
            for (int i = 1; i <= totalCnt; i++) {
                stmt.setString(1, String.format("CSE%07d", 4999 + i));
                stmt.addBatch();
                if (i % 500 == 0) {
                    stmt.executeBatch();
                    stmt.clearBatch();
                }
            }
            t2 = System.currentTimeMillis();
            totalTime = t2 - t1;
            System.out.printf("Insertion done, %d operations/sec\ntime: %.2fms\n\n", Math.round(totalCnt * 1000.0 / totalTime), totalTime);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void selectOption(int totalCnt) {
        double totalTime, t1 = System.currentTimeMillis(), t2;
        StringBuilder s = new StringBuilder();
        try {
            Statement statement = con.createStatement();
            for (int i = 0; i < totalCnt; i++) {
                String sql = String.format("select * from contract_info where contract_number = 'CSE%07d'", i);
                resultSet = statement.executeQuery(sql);
//                while (resultSet.next()) {
//                    s.append(String.format("%11s", resultSet.getString(1)));
//                    s.append(String.format("%42s", resultSet.getString(2)));
//                    s.append(String.format("%20s", resultSet.getString(3)));
//                    s.append(String.format("%28s", resultSet.getString(4)));
//                    s.append(String.format("%9s", (resultSet.getString(5))));
//                    s.append(String.format("%31s", resultSet.getString(6)));
//                    s.append(String.format("%8s", resultSet.getString(7)));
//                    s.append(resultSet.getString(8));
//                    s.append(resultSet.getString(9));
//                    s.append(resultSet.getString(10));
//                    s.append(resultSet.getString(11));
//                    s.append(resultSet.getString(12));
//                    s.append(resultSet.getString(13));
//                    s.append(resultSet.getString(14));
//                    s.append(resultSet.getString(15));
//                    s.append(resultSet.getString(16));
//                    s.append(resultSet.getString(17));
//                    s.append(resultSet.getString(18));
//                    s.append(resultSet.getString(19));
//                    s.append(resultSet.getString(20)).append("\n");
//                }
            }
            t2 = System.currentTimeMillis();
            totalTime = t2 - t1;
//            System.out.println(s + "\n");
            System.out.printf("Selection done: %d operations/sec\ntime: %.2fms\n\n", Math.round(1000.0 * totalCnt / totalTime), totalTime);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void updateOption(int totalCnt) {
        double totalTime, t1 = System.currentTimeMillis(), t2;
        try {
            String sql = "update contract_info set product_model = 'PosterFRANXX' where contract_number = (?);";
            PreparedStatement preparedStatement = con.prepareStatement(sql);
            for (int i = 0; i < totalCnt; i++) {
                preparedStatement.setString(1, String.format("CSE%07d", 5000 + i));
                preparedStatement.addBatch();
                if ((i + 1) % 500 == 0) {
                    preparedStatement.executeBatch();
                    preparedStatement.clearBatch();
                }
            }
            t2 = System.currentTimeMillis();
            totalTime = t2 - t1;
            System.out.printf("Update done: %d operations/sec\ntime: %.2fms\n\n", Math.round(1000.0 * totalCnt / totalTime), totalTime);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void deleteOption(int totalCnt) {
        double totalTime, t1 = System.currentTimeMillis(), t2;
        try {
            String sql = "delete from contract_info where contract_number = (?);";
            PreparedStatement stmt = con.prepareStatement(sql);
            for (int i = 0; i < totalCnt; i++) {
                stmt.setString(1, String.format("CSE%07d", 5000 + i));
                stmt.addBatch();
                if ((i + 1) % 500 == 0) {
                    stmt.executeBatch();
                    stmt.clearBatch();
                }
            }
            t2 = System.currentTimeMillis();
            totalTime = t2 - t1;
            System.out.printf("Delete done: %d operations/sec\ntime: %.2fms\n\n", Math.round(1000.0 * totalCnt / totalTime), totalTime);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
