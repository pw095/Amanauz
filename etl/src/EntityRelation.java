import org.data.AbstractEntity;

import java.sql.*;

public interface EntityRelation /*extends AbstractEntity */{
/*
    @Override
    public default <T extends QueueElement> T postLoad(T queueElement) {
        queueElement = AbstractEntity.super.postLoad(queueElement);

        queueElement.setEntityEffToDt(queueElement.getEntityEffToDt());
        queueElement.setEntityEffFromDt(queueElement.getEntityEffFromDt());
        queueElement.setEntityLoadMode(queueElement.getEntityLoadMode());

        String sqlSelectString = "SELECT elq_id FROM tbl_entity_load_queue WHERE elq_flow_id = ? AND elq_elm_id = ?";
        String sqlInsertString = "INSERT INTO tbl_relation_load_queue(rlq_elq_id, rlq_mode, rlq_effective_from_dt, rlq_effective_to_dt) VALUES (?, ?, ?, ?)";

        try(Connection conn = DriverManager.getConnection(Flow.connString);
            PreparedStatement preparedStatement1 = conn.prepareStatement(sqlSelectString);
            PreparedStatement preparedStatement2 = conn.prepareStatement(sqlInsertString)) {

            preparedStatement1.setInt(1, queueElement.getFlowId());
            preparedStatement1.setInt(2, queueElement.getElmId());
            int entityLoadQueueId = -1;

            try(ResultSet rs = preparedStatement1.executeQuery()) {
                if (rs.next()) {
                    entityLoadQueueId = rs.getInt("elq_id");
                    System.out.println("flowId = " + queueElement.getFlowId() + " elm_id = " + queueElement.getElmId() + " entityLoadQueueId " + entityLoadQueueId);
                }
            }

            preparedStatement2.setInt(1, entityLoadQueueId);
            preparedStatement2.setString(2, queueElement.getEntityLoadMode());
            preparedStatement2.setString(3, queueElement.getEntityEffFromDt());
            preparedStatement2.setString(4, queueElement.getEntityEffToDt());

            preparedStatement2.executeUpdate();

        } catch(SQLException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
        return queueElement;
    }
 */
}
