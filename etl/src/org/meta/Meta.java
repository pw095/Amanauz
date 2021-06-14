package org.meta;

import org.data.AbstractEntity;
import org.data.PeriodEntity;
import org.data.ReplicationEntity;
import org.flow.Flow;

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

    public static synchronized long setFlowLogStart(Flow flow) {
        long flowLoadId = 0;

        String stmtSelectString = getQuery(getInstance().getProperty("metaSqlDirectory").concat("get_max_flow_log_id.sql"));
        String stmtInsertString = getQuery(getInstance().getProperty("metaSqlDirectory").concat("put_flow_log_info.sql"));
        try(Statement stmt = conn.createStatement();
            PreparedStatement pstmt = conn.prepareStatement(stmtInsertString);
            ResultSet rs = stmt.executeQuery(stmtSelectString)) {
            if (rs.next()) {
                flow.setFlowLoadId(rs.getLong("flow_id") + 1);
            } else {
                flow.setFlowLoadId(0);
            }

            flow.setFlowLogStartTimestamp(LocalDateTime.now());
            pstmt.setLong(1, flow.getFlowLoadId());
            pstmt.setString(2, LoadStatus.RUNNING.getDbStatus());
            pstmt.setString(3, flow.getFlowLogStartTimestamp().format(dateTimeFormat));

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
            pstmt.setString(2, LocalDateTime.now().format(dateTimeFormat));
            pstmt.setLong(3, flowLoadId);

            pstmt.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    public static synchronized void getPreviousEffectiveToDt(PeriodEntity entity) {
        try {
            String stmtString = getQuery(getInstance().getProperty("metaSqlDirectory").concat("get_previous_eff_to_dt.sql"));
            String rllEffectiveToDt = null;

            try (PreparedStatement stmtSelect = conn.prepareStatement(stmtString)) {
                stmtSelect.setLong(1, entity.getEntityLayerMapId());
                try (ResultSet rs = stmtSelect.executeQuery()) {
                    if (rs.next()) {
                        rllEffectiveToDt = rs.getString("rll_effective_to_dt");
                    }
                }
            }

            if (entity.getEntityLayer() == MetaLayer.STAGE) {
                entity.setEffectiveFromDt(LocalDate.parse(rllEffectiveToDt, dateFormat).minusDays(7).format(dateFormat));
            } else {
                entity.setEffectiveFromDt(LocalDate.parse(rllEffectiveToDt, dateFormat).plusDays(1).format(dateFormat));
            }

        } catch (SQLException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    public static synchronized void getCurrentEffectiveToDt(PeriodEntity entity) {
        try {
            String stmtString = getQuery(getInstance().getProperty("metaSqlDirectory").concat("get_current_eff_to_dt.sql"));
            String rllEffectiveToDt = null;

            try (PreparedStatement stmtSelect = conn.prepareStatement(stmtString)) {
                stmtSelect.setLong(1, entity.getEntityLayerMapId());
                try (ResultSet rs = stmtSelect.executeQuery()) {
                    if (rs.next()) {
                        rllEffectiveToDt = rs.getString("rll_effective_to_dt");
                    }
                }
            }

            entity.setEffectiveToDt(rllEffectiveToDt);

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
        } catch (SQLException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    public static synchronized void setEntityInfo(AbstractEntity entity) {
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
                if (entity instanceof PeriodEntity) {
                    stmtString = getQuery(getInstance().getProperty("metaSqlDirectory").concat("put_entity_relation_load_log.sql"));
                    try (PreparedStatement stmt = conn.prepareStatement(stmtString)) {
                        stmt.setLong(1, entity.getEntityLoadLogId());
                        stmt.setString(2, entity.getEntityLoadMode().getDbMode());
                        stmt.setString(3, ((PeriodEntity) entity).getEffectiveFromDt());
                        stmt.setString(4, ((PeriodEntity) entity).getEffectiveToDt());

                        stmt.executeUpdate();
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
