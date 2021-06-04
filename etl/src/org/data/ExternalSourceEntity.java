package org.data;

import java.sql.PreparedStatement;
import java.util.List;

public interface ExternalSourceEntity {
    static abstract class ExternalData {}

    public abstract void saveData(PreparedStatement stmtUpdate, List<? extends ExternalData> dataArray);
}
