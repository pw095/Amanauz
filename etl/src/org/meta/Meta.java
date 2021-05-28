package org.meta;

import org.data.AbstractEntity;
import org.data.ImoexWebSiteEntity;

import java.sql.*;
import java.time.LocalDate;
import java.time.LocalDateTime;

import static org.meta.Properties.getInstance;
import static org.util.AuxUtil.*;

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
            pstmt.setString(3, LocalDateTime.now().format(dateTimeFormat));

            pstmt.executeUpdate();

        } catch(SQLException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }

        return flowLoadId;
    }


    public static synchronized void setFlowLogFinish(long flowLoadId, LoadStatus loadStatus) {

        String stmtString = getQuery(getInstance().getProperty("metaSqlDirectory").concat("update_flow_log_info.sql"));
        System.out.println("flowLoadId = " + flowLoadId);
        try (PreparedStatement pstmt = conn.prepareStatement(stmtString)) {
            pstmt.setString(1, loadStatus.getDbStatus());
            pstmt.setString(2, LocalDateTime.now().format(dateTimeFormat));
            pstmt.setLong(3, flowLoadId);

            pstmt.executeUpdate();
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
                stmtSelect.setString(1, entity.getEntityLayer().getDbLayer());
                stmtSelect.setString(2, entity.getEntityCode());
                try (ResultSet rs = stmtSelect.executeQuery()) {
                    if(rs.next()) {
                        entity.setEntityLayerMapId(rs.getLong("elm_id"));
                        entity.setEntityLoadMode(LoadMode.valueOf(rs.getString("elm_mode").toUpperCase()));
                    } else {
                        throw new RuntimeException("No such Entity!");
                    }
                }
            }

            stmtString = getQuery(getInstance().getProperty("metaSqlDirectory").concat("get_iteration_number.sql"));

            try (PreparedStatement stmtSelect = conn.prepareStatement(stmtString)) {
                stmtSelect.setLong(1, entity.getEntityLayerMapId());
                try (ResultSet rs = stmtSelect.executeQuery()) {
                    if (rs.next()) {
                        entity.setIterationNumber(rs.getLong("ell_iteration_number")+1);
                    } else {
                        entity.setIterationNumber(1);
                        if (entity.getEntityLoadMode() == LoadMode.INCR) {
                            throw new RuntimeException("No full load found!");
                        }
                    }
                }
            }
            String rllEffectiveToDt = "2021-04-30";
            if (entity.getEntityLoadMode() == LoadMode.INCR) {
                stmtString = getQuery(getInstance().getProperty("metaSqlDirectory").concat("get_previous_eff_to_dt.sql"));
                try (PreparedStatement stmtSelect = conn.prepareStatement(stmtString)) {
                    stmtSelect.setLong(1, entity.getEntityLayerMapId());
                    try (ResultSet rs = stmtSelect.executeQuery()) {
                        if (rs.next()) {
                            rllEffectiveToDt = rs.getString("rll_effective_to_dt");
                        }
                    }
                }
            }
            if (entity instanceof ImoexWebSiteEntity) {
                ((ImoexWebSiteEntity) entity).setPreviousEffectiveToDt(rllEffectiveToDt);
                ((ImoexWebSiteEntity) entity).setEffectiveFromDt(LocalDate.parse(rllEffectiveToDt, dateFormat).plusDays(1).format(dateFormat));
            }
        } catch (SQLException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    public static synchronized void setEntityInfo(AbstractEntity entity) {
        System.out.println("elm = " + entity.getEntityLayerMapId());
        System.out.println("flow_load_id = " + entity.getFlowLoadId());
        System.out.println("iteration_number = " + entity.getIterationNumber());
        try {
            String stmtString = getQuery(getInstance().getProperty("metaSqlDirectory").concat("put_entity_load_info.sql"));
            try(PreparedStatement stmt = conn.prepareStatement(stmtString)) {
                stmt.setLong(1, entity.getFlowLoadId());
                stmt.setLong(2, entity.getEntityLayerMapId());
                stmt.setLong(3, entity.getIterationNumber());
                stmt.setLong(4, entity.getInsertCount());
                stmt.setLong(5, entity.getUpdateCount());
                stmt.setLong(6, entity.getDeleteCount());
                stmt.setString(7, LoadStatus.SUCCEEDED.getDbStatus());
                stmt.setString(8, entity.getEntityStartLoadTimestamp().format(dateTimeFormat));
                stmt.setString(9, entity.getEntityFinishLoadTimestamp().format(dateTimeFormat));
                stmt.setString(10, "");
                stmt.executeUpdate();
            }

            stmtString = getQuery(getInstance().getProperty("metaSqlDirectory").concat("get_entity_load_log_id.sql"));

            try (PreparedStatement stmtSelect = conn.prepareStatement(stmtString)) {
                stmtSelect.setLong(1, entity.getFlowLoadId());
                stmtSelect.setLong(2, entity.getEntityLayerMapId());
                try (ResultSet rs = stmtSelect.executeQuery()) {
                    if (rs.next()) {
                        entity.setEntityLoadLogId(rs.getLong("ell_id"));
                    } else {
                        throw new RuntimeException("Дребедень какая-то!");
                    }
                }
            }

            if (entity.getInsertCount() + entity.getUpdateCount() + entity.getDeleteCount() > 0) {
                if (entity instanceof ImoexWebSiteEntity) {
                    stmtString = getQuery(getInstance().getProperty("metaSqlDirectory").concat("put_entity_relation_load_log.sql"));
                    try (PreparedStatement stmt = conn.prepareStatement(stmtString)) {
                        stmt.setLong(1, entity.getEntityLoadLogId());
                        stmt.setString(2, entity.getEntityLoadMode().getDbMode());
                        stmt.setString(3, ((ImoexWebSiteEntity) entity).getEffectiveFromDt());
                        stmt.setString(4, ((ImoexWebSiteEntity) entity).getEffectiveToDt());

                        stmt.executeUpdate();
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
/*
    public static synchronized void setEntityRelationInfo(AbstractEntity entity) {
        try {
            String stmtString = getQuery(getInstance().getProperty("metaSqlDirectory").concat("put_entity_relation_load_log.sql"));
            try(PreparedStatement stmt = conn.prepareStatement(stmtString)) {
                stmt.setLong(1, entity.getEntityLoadLogId());
                stmt.setString(2, entity.getEntityLoadMode().getDbMode());
                stmt.setString(3, ((ImoexWebSiteEntity) entity).getEffectiveFromDt());
                stmt.setString(4, ((ImoexWebSiteEntity) entity).getEffectiveToDt());

                stmt.executeUpdate();
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static synchronized void getEntityRelationEffectiveDt(AbstractEntity entity) {
        try {
            String stmtString = getQuery(getInstance().getProperty("metaSqlDirectory").concat("get_entity_relation_effective_dt.sql"));
            try(PreparedStatement stmt = conn.prepareStatement(stmtString)) {
                stmt.setLong(1, entity.getEntityLoadLogId());
                stmt.setString(2, entity.getEntityLoadMode().getDbMode());
                stmt.setString(3, ((ImoexWebSiteEntity) entity).getEffectiveFromDt());
                stmt.setString(4, ((ImoexWebSiteEntity) entity).getEffectiveToDt());

                stmt.executeUpdate();
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    */
}
