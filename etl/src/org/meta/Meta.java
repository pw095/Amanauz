package org.meta;

import org.data.AbstractEntity;

import java.sql.*;
import java.time.LocalDateTime;

import static org.util.AuxUtil.getQuery;
import static org.util.AuxUtil.dateFormat;
import static org.meta.Properties.getInstance;
public final class Meta {

    private static Connection conn;

    static {
        try {
            conn = DriverManager.getConnection(getInstance().getProperty("metaUrl"));
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static synchronized long setFlowLogStart() {
        long flowLoadId = 0;

//        String stmtString = getQuery(getInstance().getProperty("meteSqlDirectory").concat("set_flow_log_start.sql"));

//        String sqlSelectString = "SELECT MAX(flow_id) AS flow_id FROM tbl_flow_log";
//        String sqlInsertString = "INSERT INTO tbl_flow_log(flow_id, flow_status, flow_start_dttm) VALUES(?, ?, ?)";
        String stmtSelectString = getQuery(getInstance().getProperty("metaSqlDirectory").concat("get_max_flow_log_id.sql"));
        String stmtInsertString = getQuery(getInstance().getProperty("metaSqlDirectory").concat("put_flow_log_info.sql"));
        try(Statement stmt = conn.createStatement();
            PreparedStatement pstmt = conn.prepareStatement(stmtInsertString);
            ResultSet rs = stmt.executeQuery(stmtSelectString)) {
            if (rs.next()) {
                flowLoadId = rs.getLong("flow_id");
            } else {
                flowLoadId = 0;
            }

            pstmt.setLong(1, ++flowLoadId);
            pstmt.setString(2, LoadStatus.RUNNING.getDbStatus());
            pstmt.setString(3, LocalDateTime.now().format(dateFormat));

            pstmt.executeUpdate();

        } catch(SQLException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }

        return flowLoadId;
    }


    public static synchronized void setFlowLogFinish(long flowLoadId, LoadStatus loadStatus) {

        String stmtString = getQuery(getInstance().getProperty("metaSqlDirectory").concat("update_flow_log_info.sql"));

        try (PreparedStatement pstmt = conn.prepareStatement(stmtString)) {
            pstmt.setString(1, loadStatus.getDbStatus());
            pstmt.setString(2, LocalDateTime.now().format(dateFormat));
            pstmt.setLong(3, flowLoadId);
        } catch (SQLException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    public static synchronized void getEntityLayerMapInfo(AbstractEntity entity) {
        try {
            String stmtString;

            stmtString = getQuery(getInstance().getProperty("metaSqlDirectory").concat("get_entity_layer_map_info.sql"));

            try (PreparedStatement stmtSelect = conn.prepareStatement(stmtString)) {
                stmtSelect.setString(1, entity.getEntityLayer());
                stmtSelect.setString(2, entity.getEntityCode());
                try (ResultSet rs = stmtSelect.executeQuery()) {
                    if(rs.next()) {
                        entity.setEntityLayerId(rs.getLong("elm_id"));
                        entity.setEntityLoadMode(rs.getString("elm_mode"));
                    } else {
                        throw new RuntimeException("No such Entity!");
                    }
                }
            }

            stmtString = getQuery(getInstance().getProperty("metaSqlDirectory").concat("get_iteration_number.sql"));

            try (PreparedStatement stmtSelect = conn.prepareStatement(stmtString)) {
                stmtSelect.setLong(1, entity.getEntityLayerId());
                try (ResultSet rs = stmtSelect.executeQuery()) {
                    if (rs.next()) {
                        entity.setIterationNumber(rs.getLong("iterationNumber"));
                    } else {
                        entity.setIterationNumber(1);
                        if (entity.getEntityLoadMode().equals("INCR")) {
                            throw new RuntimeException("No full load found!");
                        }
                    }
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }
}
