package org.data;

import org.flow.Flow;
import org.meta.LoadMode;
import org.meta.Meta;
import org.meta.MetaLayer;
import org.sqlite.SQLiteConfig;

import java.sql.*;
import java.time.LocalDateTime;
import java.util.concurrent.Callable;

import static org.meta.Properties.getInstance;
import static org.util.AuxUtil.getQuery;

public abstract class AbstractEntity implements Callable<AbstractEntity> {

    private static final String dataAuxDir = "auxiliary/";
    static final String dataInsertScript = "$$entity_name_insert.sql";
    String sqlDirectory;

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

//    protected abstract void detailLoad(PreparedStatement stmtUpdate);
    protected abstract void callLoad(Connection conn);

//    protected void preLoad(Connection conn) {}
/*

    private String getTechTruncateSQL() {
        String lReturn = (sqlDirectory + dataAuxDir) + dataTruncScript.replace("$$entity_name", "tech$" + this.entityCode);
        return lReturn;
    }
*/
/*

    private String getTechSingleSQL() {
        String lReturn = (sqlDirectory + this.getEntityLoadMode().getDbMode() + '/') + dataInsertScript.replace("$$entity_name", "tech$" + this.entityCode);
        return lReturn;
    }
*/
/*

    private String getSingleSQL() {
        String lReturn = (sqlDirectory + this.getEntityLoadMode().getDbMode() + '/') + dataSingleScript.replace("$$entity_name", this.entityCode);
        return lReturn;
    }
*/

    public LoadMode getEntityLoadMode() {
        return this.entityLoadMode;
    }
    public void setEntityLoadMode(LoadMode entityLoadMode) {
        this.entityLoadMode = entityLoadMode;
    }

    public MetaLayer getEntityLayer() {
        return this.entityLayer;
    }
    public void setEntityLayer(MetaLayer entityLayer) {
        this.entityLayer = entityLayer;
    }
    public String getEntityCode() {
        return this.entityCode;
    }

    public String getSqlDirectory() { return this.sqlDirectory; }
    public String getDataAuxDir() { return this.dataAuxDir; }

    protected AbstractEntity(Flow flow, MetaLayer entityLayer, String entityCode) {
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
            SQLiteConfig config = new SQLiteConfig();
            config.enableLoadExtension(true);
            connTarget = DriverManager.getConnection(getInstance().getProperty(this.entityLayer.toString().concat("_targetURL")), config.toProperties());
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public AbstractEntity call() {

        this.setEntityStartLoadTimestamp(LocalDateTime.now());
        callLoad(connTarget);
//        preLoad(connTarget);
/*
        try {

//            String techTruncateSQL = getQuery(getTechTruncateSQL());

            if (techTruncateSQL != null) {
                try (PreparedStatement stmt = connTarget.prepareStatement(techTruncateSQL)) {
                    detailLoad(stmt);
                }
            }
            String techSingleSQL = getQuery(getTechSingleSQL());
            if (techSingleSQL != null) {
                try (PreparedStatement stmt = connTarget.prepareStatement(techSingleSQL)) {
                    detailLoad(stmt);
                }
            }
            String singleSQL = getQuery(getSingleSQL());
            if (singleSQL != null) {
                try (PreparedStatement stmt = connTarget.prepareStatement(singleSQL)) {
                    detailLoad(stmt);
                }
            }
//            concreteLoad(connTarget);
            String insertSQL = getQuery(getInsertSQL());
            if (insertSQL != null) {
                try (PreparedStatement stmt = connTarget.prepareStatement(insertSQL)) {
                    detailLoad(stmt);
                }
            }
        } catch (Exception e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }

 */
        this.setEntityFinishLoadTimestamp(LocalDateTime.now());
        Meta.setEntityInfo(this);
        return this;
    }
}
