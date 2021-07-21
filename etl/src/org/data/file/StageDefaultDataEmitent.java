package org.data.file;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.flow.Flow;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;

import static org.util.AuxUtil.dateTimeFormat;

public class StageDefaultDataEmitent extends org.data.FileEntity implements ExcelEntity {

    static class StageDefaultDataEmitentData extends ExternalData {
        String emitentCode;
        String emitentName;

        public StageDefaultDataEmitentData(String emitentCode, String emitentName) {
            this.emitentCode = emitentCode;
            this.emitentName = emitentName;
        }
    }

    public StageDefaultDataEmitent(Flow flow) {
        super(flow, "default_data_emitent", "DefaultDataEmitent.xlsx");
    }

    @Override
    public void callLoad(Connection conn) {
        concreteLoad(conn);
    }

    public StageDefaultDataEmitentData readRow(Row row) {
        Iterator<Cell> cellIterator = row.iterator();
        String emitentCode = null;
        String emitentName = null;
        while (cellIterator.hasNext()) {
            Cell cell = cellIterator.next();
            switch (cell.getColumnIndex()) {
                case 0:
                    emitentCode = cell.getStringCellValue();
                    break;
                case 1:
                    emitentName = cell.getStringCellValue();
                    break;
                default:
                    throw new RuntimeException("Invalid column index");
            }
        }
        return new StageDefaultDataEmitentData(emitentCode, emitentName);
    }

    public void saveData(PreparedStatement stmtUpdate, List<? extends ExternalData> dataArray) {
        try {
            for(ExternalData iter : dataArray) {
                StageDefaultDataEmitentData jter = (StageDefaultDataEmitentData) iter;
                stmtUpdate.setLong(1, this.getFlowLoadId());
                stmtUpdate.setString(2, this.getFlowLogStartTimestamp().format(dateTimeFormat));
                stmtUpdate.setString(3, jter.emitentCode);
                stmtUpdate.setString(4, jter.emitentName);
                stmtUpdate.addBatch();
            }
            setInsertCount(getInsertCount() + stmtUpdate.executeBatch().length);
            stmtUpdate.clearBatch();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

}
