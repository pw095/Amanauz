package org.data;

import org.flow.Flow;
import org.meta.LoadMode;
import org.meta.Meta;
import org.meta.MetaLayer;

import java.sql.*;
import java.time.LocalDateTime;
import java.util.concurrent.Callable;

import static org.meta.Properties.getInstance;
import static org.util.AuxUtil.getQuery;

public abstract class AbstractEntity implements Callable<AbstractEntity> {

    static private final String dataAuxDir = "auxiliary/";
    static private final String dataTruncScript = "$$entity_name_truncate.sql";
    static private final String dataInsertScript = "$$entity_name_insert.sql";
    private String sqlDirectory;

    MetaLayer entityLayer;
    String entityCode;
    LoadMode entityLoadMode;

    private long entityLayerMapId;
    private long entityLoadLogId;
    private long flowLoadId;
    private LocalDateTime flowLogStartTimestamp;
    private long iterationNumber;

    public long getEntityLayerMapId() {
        return this.entityLayerMapId;
    }
    public void setEntityLayerMapId(long entityLayerMapId) {
        this.entityLayerMapId = entityLayerMapId;
    }
    public long getEntityLoadLogId() {
        return this.entityLoadLogId;
    }
    public void setEntityLoadLogId(long entityLoadLogId) {
        this.entityLoadLogId = entityLoadLogId;
    }
    public long getFlowLoadId() {
        return this.flowLoadId;
    }
    public void setFlowLoadId(long flowLoadId) {
        this.flowLoadId = flowLoadId;
    }
    public LocalDateTime getFlowLogStartTimestamp() {
        return this.flowLogStartTimestamp;
    }
    public void setFlowLogStartTimestamp(LocalDateTime flowLogStartTimestamp) {
        this.flowLogStartTimestamp = flowLogStartTimestamp;
    }

    public long getIterationNumber() {
        return this.iterationNumber;
    }
    public void setIterationNumber(long iterationNumber) {
        this.iterationNumber = iterationNumber;
    }

    private long insertCount = 0;
    private long updateCount = 0;
    private long deleteCount = 0;

    public void setInsertCount(long insertCount) {
        this.insertCount = insertCount;
    }
    public long getInsertCount() {
        return this.insertCount;
    }

    public void setUpdateCount(long updateCount) {
        this.updateCount = updateCount;
    }
    public long getUpdateCount() {
        return this.updateCount;
    }

    public void setDeleteCount(long deleteCount) {
        this.deleteCount = deleteCount;
    }
    public long getDeleteCount() {
        return this.deleteCount;
    }



    private LocalDateTime entityStartLoadTimestamp;
    private LocalDateTime entityFinishLoadTimestamp;

    public void setEntityStartLoadTimestamp(LocalDateTime loadTimestamp) {
        this.entityStartLoadTimestamp = loadTimestamp;
    }
    public LocalDateTime getEntityStartLoadTimestamp() {
        return this.entityStartLoadTimestamp;
    }

    public void setEntityFinishLoadTimestamp(LocalDateTime loadTimestamp) {
        this.entityFinishLoadTimestamp = loadTimestamp;
    }
    public LocalDateTime getEntityFinishLoadTimestamp() {
        return this.entityFinishLoadTimestamp;
    }

    private Connection connTarget;

    protected abstract void detailLoad(PreparedStatement stmtUpdate);

    private String getTruncateSQL() {
        String lReturn = (sqlDirectory + dataAuxDir) + dataTruncScript.replace("$$entity_name", this.entityCode);
        return lReturn;
    }

    private String getInsertSQL() {
        String lReturn = (sqlDirectory + dataAuxDir) + dataInsertScript.replace("$$entity_name", this.entityCode);
        return lReturn;
    }

    public LoadMode getEntityLoadMode() {
        return this.entityLoadMode;
    }
    public void setEntityLoadMode(LoadMode entityLoadMode) {
        this.entityLoadMode = entityLoadMode;
    }

    public MetaLayer getEntityLayer() {
        return this.entityLayer;
    }
    public String getEntityCode() {
        return this.entityCode;
    }

    AbstractEntity(Flow flow, MetaLayer entityLayer, String entityCode) {
        setFlowLoadId(flow.getFlowLoadId());
        setFlowLogStartTimestamp(flow.getFlowLogStartTimestamp());
        this.entityLayer = entityLayer;
        this.entityCode = entityCode;

        Meta.getEntityLayerMapInfo(this);

        try {
            sqlDirectory = getInstance().getProperty("dataSqlDirectory").concat(this.entityLayer.getDbLayer()).concat("/").concat(this.entityCode).concat("/sql/");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        try {
            connTarget = DriverManager.getConnection(getInstance().getProperty(this.entityLayer.toString().concat("_targetURL")));
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public AbstractEntity call() {

        this.setEntityStartLoadTimestamp(LocalDateTime.now());
        String insertSQL = getQuery(getInsertSQL());
        try {
            try(PreparedStatement stmtUpdate = connTarget.prepareStatement(insertSQL)) {
                detailLoad(stmtUpdate);
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
        this.setEntityFinishLoadTimestamp(LocalDateTime.now());
        Meta.setEntityInfo(this);
        return this;
    }
}