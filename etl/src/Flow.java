import java.sql.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class Flow {

    private static final String connString = "jdbc:sqlite:../db/file/meta.db";
    private static final DateTimeFormatter dateFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    private int flowId;
    private LoadStatus loadStatus;
    private LocalDateTime startDttm;
    private LocalDateTime finishDttm;

    public void startFlow() {

        String sqlSelectString = "SELECT MAX(flow_id) AS flow_id FROM tbl_flow_log";
        String sqlInsertString = "INSERT INTO tbl_flow_log(flow_id, flow_status, flow_start_dttm) VALUES(?, ?, ?)";
        try(Connection conn = DriverManager.getConnection(connString);
            Statement stmt = conn.createStatement();
            PreparedStatement preparedStatement = conn.prepareStatement(sqlInsertString);
            ResultSet rs = stmt.executeQuery(sqlSelectString)) {
              if (rs.next()) {
                  flowId = rs.getInt("flow_id");
              } else {
                  flowId = 0;
              }

              flowId++;
              loadStatus = LoadStatus.RUNNING;
              startDttm = LocalDateTime.now();

              preparedStatement.setInt(1, flowId);
              preparedStatement.setString(2, loadStatus.getDbStatus());
              preparedStatement.setString(3, startDttm.format(dateFormat));

              preparedStatement.executeUpdate();

        } catch(SQLException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }

    }

    public void finishFlow() {

        String sqlUpdateString = "UPDATE tbl_flow_log SET flow_status = ?, flow_finish_dttm = ? WHERE flow_status = 'running' AND flow_id = ?";
        try(Connection conn = DriverManager.getConnection(connString);
            PreparedStatement preparedStatement = conn.prepareStatement(sqlUpdateString)) {

            loadStatus = LoadStatus.SUCCEEDED;
            finishDttm = LocalDateTime.now();

            preparedStatement.setString(1, loadStatus.getDbStatus());
            preparedStatement.setString(2, finishDttm.format(dateFormat));
            preparedStatement.setInt(3, flowId);

            preparedStatement.executeUpdate();

        } catch(SQLException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    public void terminateFlow() {

        String sqlUpdateString = "UPDATE tbl_flow_log SET flow_status = ?, flow_finish_dttm = ? WHERE flow_status = 'running' AND flow_id = ?";
        try(Connection conn = DriverManager.getConnection(connString);
            PreparedStatement preparedStatement = conn.prepareStatement(sqlUpdateString)) {

            loadStatus = LoadStatus.FAILED;
            finishDttm = LocalDateTime.now();

            preparedStatement.setString(1, loadStatus.getDbStatus());
            preparedStatement.setString(2, finishDttm.format(dateFormat));
            preparedStatement.setInt(3, flowId);

            preparedStatement.executeUpdate();

        } catch(SQLException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    public static void main(String[] args) throws InterruptedException {
        Flow fl = new Flow();
        fl.startFlow();
        new Thread().sleep(10_000);
        fl.finishFlow();
//        fl.terminateFlow();
    }
}
