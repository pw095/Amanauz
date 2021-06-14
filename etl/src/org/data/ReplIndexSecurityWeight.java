package org.data;

import org.flow.Flow;
import org.meta.MetaLayer;
import org.util.AuxUtil;

import java.sql.*;


import static org.meta.Properties.getInstance;
import static org.util.AuxUtil.dateTimeFormat;

public class ReplIndexSecurityWeight {
/*
    static private final String attachDatabaseScript = "attach_database.sql";
    static private final String loadExtensionScript = "load_extension.sql";
    static private String etlSqlDirectory;
    public ReplIndexSecurityWeight(Flow flow) {
        super(flow, "index_security_weight");
        setEntityLayer(MetaLayer.REPL);
        try {
            etlSqlDirectory = getInstance().getProperty("dataSqlDirectory");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private String getAttachSQL() {
        String lReturn = etlSqlDirectory + dataAuxDir + attachDatabaseScript;
        return lReturn;
    }

    private String getLoadExtensionSQL() {
        String lReturn = etlSqlDirectory + dataAuxDir + loadExtensionScript;
        return lReturn;
    }

    public void preLoad(Connection conn) {
        System.out.println(getAttachSQL());
//        System.out.println(AuxUtil.getQuery(getAttachSQL()).replace("$$path", getInstance().getProperty("REPL_sourceFILE")));
        System.out.println(AuxUtil.getQuery(getLoadExtensionSQL()).replace("$$path", getInstance().getProperty("hashFunctionPath")));
        try(Statement stmt = conn.createStatement()) {
//            stmt.execute(AuxUtil.getQuery(getAttachSQL()).replace("$$path", getInstance().getProperty("REPL_sourceFILE")));
            stmt.execute(AuxUtil.getQuery(getLoadExtensionSQL()).replace("$$path", getInstance().getProperty("hashFunctionPath")));
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void detailLoad(PreparedStatement stmtUpdate) {
        try {
            stmtUpdate.setString('1900-01-01');
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
*/

}
