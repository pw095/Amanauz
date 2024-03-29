package org.data;

import org.flow.Flow;
import org.meta.MetaLayer;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

import static org.meta.Properties.getInstance;
import static org.util.AuxUtil.getQuery;

public class ReplicationEntity extends PeriodEntity {

    static private final String attachDatabaseScript = "attach_database.sql";
    static private final String loadExtensionScript = "load_extension.sql";
    static private final String dataTruncScript = "$$entity_name_truncate.sql";
    static private final String dataSingleScript = "$$entity_name.sql";
    static private final String dataUpdateScript = "$$entity_name_update.sql";
    static private String etlSqlDirectory;

    private String getTechTruncateSQL() {
        String lReturn = (sqlDirectory + getDataAuxDir()) + dataTruncScript.replace("$$entity_name", "tech$" + getEntityCode());
        return lReturn;
    }

    private String getAttachSQL() {
        String lReturn = etlSqlDirectory + attachDatabaseScript;
        return lReturn;
    }

    private String getLoadExtensionSQL() {
        String lReturn = etlSqlDirectory + loadExtensionScript;
        return lReturn;
    }

    private String getTechSingleSQL() {
        String lReturn = getSqlDirectory() /*+ this.getEntityLoadMode().getDbMode() + '/'*/ + dataSingleScript.replace("$$entity_name", "tech$" + getEntityCode());
        return lReturn;
    }

    private String getSingleSQL() {
        String lReturn = sqlDirectory /*+ this.getEntityLoadMode().getDbMode() + '/'*/ + dataSingleScript.replace("$$entity_name", getEntityCode());
        return lReturn;
    }

    private String getUpdateSQL() {
        String lReturn = sqlDirectory + dataUpdateScript.replace("$$entity_name", getEntityCode());
        return lReturn;
    }

    public ReplicationEntity(Flow flow, String entityCode) {
        super(flow, MetaLayer.REPL, entityCode);
        try {
            etlSqlDirectory = getInstance().getProperty("dataSqlDirectory") + getDataAuxDir();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void callLoad(Connection conn) {

        String sqlStmtToExecute;
        try {
            try (Statement stmt = conn.createStatement()) {
                // truncate tech
                sqlStmtToExecute = getQuery(getTechTruncateSQL());
                stmt.execute(sqlStmtToExecute);

                // attach db
                sqlStmtToExecute = getQuery(getAttachSQL()).replace("$$path", getInstance().getProperty("REPL_sourceFILE"));
                stmt.execute(sqlStmtToExecute);

                // load extension
                sqlStmtToExecute = getQuery(getLoadExtensionSQL()).replace("$$path", getInstance().getProperty("hashFunctionPath"));
                stmt.execute(sqlStmtToExecute);
            }

            // insert tech
            sqlStmtToExecute = getQuery(getTechSingleSQL());
            try (PreparedStatement stmt = conn.prepareStatement(sqlStmtToExecute)) {
                stmt.setString(1, getEffectiveFromDt());
                stmt.setLong(2, getFlowLoadId());
                stmt.executeUpdate();
            }

            // merge dest in repl
            sqlStmtToExecute = getQuery(getSingleSQL());
            try (PreparedStatement stmt = conn.prepareStatement(sqlStmtToExecute)) {
                stmt.setLong(1, getFlowLoadId());
                setInsertCount(getInsertCount() + stmt.executeUpdate());
            }

            // update SQL
            sqlStmtToExecute = getQuery(getUpdateSQL());
            try (PreparedStatement stmt = conn.prepareStatement(sqlStmtToExecute)) {
                stmt.executeUpdate();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
