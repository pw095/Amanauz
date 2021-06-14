package org.data;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

import static org.util.AuxUtil.getQuery;

public interface StageEntity {

    static final String dataInsertScript = "$$entity_name_insert.sql";

    static abstract class ExternalData {}

    public abstract String getSqlDirectory();
    public abstract String getDataAuxDir();
    public abstract String getEntityCode();

    public abstract void saveData(PreparedStatement stmtUpdate, List<? extends ExternalData> dataArray);

    public abstract void detailLoad(PreparedStatement stmt);

    default String getInsertSQL() {
        String lReturn = (getSqlDirectory() + getDataAuxDir()) + dataInsertScript.replace("$$entity_name", getEntityCode());
        return lReturn;
    }

    default void concreteLoad(Connection conn) {
        String insertSQL = getQuery(getInsertSQL());
        if (insertSQL != null) {
            try (PreparedStatement stmt = conn.prepareStatement(insertSQL)) {
                detailLoad(stmt);
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
}
