import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.LocalDateTime;

public interface Entity {
    public abstract QueueElement load(QueueElement queueElement);

    public default <T extends QueueElement> T preLoad(T queueElement) {
        queueElement.setLoadStatus(LoadStatus.RUNNING);
        queueElement.setStartDttm(LocalDateTime.now());

        String sqlUpdateString = "UPDATE tbl_entity_load_queue SET elq_status = ?, elq_start_dttm = ? WHERE elq_flow_id = ? AND elq_elm_id = ?";
        try(Connection conn = DriverManager.getConnection(Flow.connString);
            PreparedStatement preparedStatement = conn.prepareStatement(sqlUpdateString)) {

            preparedStatement.setString(1, queueElement.getLoadStatus().getDbStatus());
            preparedStatement.setString(2, queueElement.getStartDttm().format(Flow.dateFormat));
            preparedStatement.setInt(3, queueElement.getFlowId());
            preparedStatement.setInt(4, queueElement.getElmId());

            preparedStatement.executeUpdate();

        } catch(SQLException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }

        return queueElement;
    }

    public default <T extends QueueElement> T postLoad(T queueElement) {

        queueElement.setLoadStatus(LoadStatus.SUCCEEDED);
        queueElement.setFinishDttm(LocalDateTime.now());

        String sqlUpdateString = "UPDATE tbl_entity_load_queue SET elq_status = ?, elq_finish_dttm = ? WHERE elq_flow_id = ? AND elq_elm_id = ?";
        try(Connection conn = DriverManager.getConnection(Flow.connString);
            PreparedStatement preparedStatement = conn.prepareStatement(sqlUpdateString)) {

            preparedStatement.setString(1, queueElement.getLoadStatus().getDbStatus());
            preparedStatement.setString(2, queueElement.getFinishDttm().format(Flow.dateFormat));
            preparedStatement.setInt(3, queueElement.getFlowId());
            preparedStatement.setInt(4, queueElement.getElmId());

            preparedStatement.executeUpdate();

        } catch(SQLException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }

        return queueElement;
    }
}
