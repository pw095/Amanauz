package org.data;

import java.sql.*;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.concurrent.Callable;

import static org.meta.Properties.getInstance;
import static org.util.AuxUtil.getQuery;

public abstract class AbstractEntity implements Callable<AbstractEntity> {

    static private final String dataAuxDir = "auxiliary/";
    static private final String dataTruncScript = "$$entity_name_truncate.sql";
    static private final String dataInsertScript = "$$entity_name_insert.sql";
    static private final String dataSelectString = "$$entity_name.sql"; // move to relation
    static private String sqlDirectory;

    private long entityLayerId;
    long flowLoadId;
    long iterationNumber;
    long insertCount;
    private String entityLoadMode;
    String entityTruncateOption;
    String entityLayer;
    String entityCode;

    LocalDateTime entityStartLoadTimestamp;
    LocalDateTime entityFinishLoadTimestamp;

    LocalDate effectiveFromDt; // move to relation
    LocalDate effectiveToDt; // move to relation

    private Connection connSource; // move to relation
    private Connection connTarget;

    protected abstract void detailLoad(PreparedStatement stmtUpdate);

    private String getTruncateSQL() {
        String lReturn = (sqlDirectory + dataAuxDir + dataTruncScript).replace("$$entity_name", this.entityCode);
        return lReturn;
    }

    private String getInsertSQL() {
        String lReturn = (sqlDirectory + dataAuxDir + dataInsertScript).replace("$$entity_name", this.entityCode);
        return lReturn;
    }

    public long getEntityLayerId() {
        return this.entityLayerId;
    }
    public void setEntityLayerId(long entityLayerId) {
        this.entityLayerId = entityLayerId;
    }

    public String getEntityLoadMode() {
        return this.entityLoadMode;
    }
    public void setEntityLoadMode(String entityLoadMode) {
        this.entityLoadMode = entityLoadMode;
    }

    public long getIterationNumber() {
        return this.iterationNumber;
    }
    public void setIterationNumber(long iterationNumber) {
        this.iterationNumber = iterationNumber;
    }

    public String getEntityLayer() {
        return this.entityLayer;
    }
    public String getEntityCode() {
        return this.entityCode;
    }
    private void customOps() {
        if (this.entityLoadMode.equals("FULL")) {
            String stmtString = getQuery(getTruncateSQL());
            try (Statement stmtTruncate = connTarget.createStatement()) {
                stmtTruncate.executeUpdate(stmtString);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
    }

    AbstractEntity(long flowLoadId) {
        this.flowLoadId = flowLoadId;

        try {
            sqlDirectory = getInstance().getProperty("dataSqlDirectory").concat("$$entity_name/sql");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        try {
            connTarget = DriverManager.getConnection(getInstance().getProperty("targetURL"));
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public AbstractEntity call() {
        String insertSQL = getQuery(getInsertSQL());
        try {
            try(PreparedStatement stmtUpdate = connTarget.prepareStatement(insertSQL)) {
                detailLoad(stmtUpdate);
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
        return this;
    }
/*
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
    }*/
}
