package org.data;

import org.flow.Flow;
import org.meta.MetaLayer;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

import static org.meta.Properties.getInstance;
import static org.util.AuxUtil.getQuery;

public class DetailDataEntity extends PeriodEntity {

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
        String lReturn = getSqlDirectory() + dataSingleScript.replace("$$entity_name", "tech$" + getEntityCode());
        return lReturn;
    }

    private String getSingleSQL() {
        String lReturn = sqlDirectory + dataSingleScript.replace("$$entity_name", getEntityCode());
        return lReturn;
    }

    private String getUpdateSQL() {
        String lReturn = sqlDirectory + dataUpdateScript.replace("$$entity_name", getEntityCode());
        return lReturn;
    }

    public DetailDataEntity(Flow flow, String entityCode) {
        super(flow, MetaLayer.DDS, entityCode);
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
                if ( sqlStmtToExecute != null ) {
                    stmt.execute(sqlStmtToExecute);
                }

                // attach db
                sqlStmtToExecute = getQuery(getAttachSQL()).replace("$$path", getInstance().getProperty("DDS_sourceFILE"));
                if ( sqlStmtToExecute != null ) {
                    stmt.execute(sqlStmtToExecute);
                }

                // load extension
                sqlStmtToExecute = getQuery(getLoadExtensionSQL()).replace("$$path", getInstance().getProperty("hashFunctionPath"));
                if ( sqlStmtToExecute != null ) {
                    stmt.execute(sqlStmtToExecute);
                }
            }

            // insert tech

            sqlStmtToExecute = getQuery(getTechSingleSQL());
            try (PreparedStatement stmt = conn.prepareStatement(sqlStmtToExecute)) {
                switch (this.getEntityCode()) {
                    case "sat_sal_emitent":
                        stmt.setString(1, getEffectiveFromDt());
                        stmt.setString(2, getEffectiveFromDt());/*
                        stmt.setString(3, getEffectiveFromDt());
                        stmt.setString(4, getEffectiveFromDt());*/
                        stmt.setLong(3, getFlowLoadId());
                        break;
                    default:
                        stmt.setLong(1, getFlowLoadId());
                        stmt.setString(2, getEffectiveFromDt());
                        break;
                }
                stmt.executeUpdate();
            }
/*            if ( sqlStmtToExecute != null ) {
                try (PreparedStatement stmt = conn.prepareStatement(sqlStmtToExecute)) {
                    stmt.setLong(1, getFlowLoadId());
                    stmt.setString(2, getEffectiveFromDt());
                    stmt.executeUpdate();
                }
            }*/

            // merge dest in dds
            sqlStmtToExecute = getQuery(getSingleSQL());
            //System.out.println()
            if ( sqlStmtToExecute != null ) {
                try (PreparedStatement stmt = conn.prepareStatement(sqlStmtToExecute)) {
                    stmt.setLong(1, getFlowLoadId());
                    setInsertCount(getInsertCount() + stmt.executeUpdate());
                }
            }

            // update SQL
            switch (this.getEntityCode()) {
                case "sat_sal_emitent":
                    sqlStmtToExecute = getQuery(getUpdateSQL());
                    try (PreparedStatement stmt = conn.prepareStatement(sqlStmtToExecute)) {
                        stmt.executeUpdate();
                    }
                    break;
                default:
                    break;
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
