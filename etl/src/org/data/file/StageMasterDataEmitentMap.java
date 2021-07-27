package org.data.file;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.flow.Flow;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;

import static org.util.AuxUtil.dateFormat;
import static org.util.AuxUtil.dateTimeFormat;

public class StageMasterDataEmitentMap extends org.data.FileEntity implements ExcelEntity {

    static class StageMasterDataEmitentMapData extends ExternalData {
        String fullName;
        String shortName;
        String sourceSystemCode;
        String emitentCode;
        String emitentShortName;

        public StageMasterDataEmitentMapData(String sourceSystemCode,
                                             String emitentCode,
                                             String emitentShortName) {
            this.sourceSystemCode = sourceSystemCode;
            this.emitentCode = emitentCode;
            this.emitentShortName = emitentShortName;
        }
    }

    public StageMasterDataEmitentMap(Flow flow) {
        super(flow, "master_data_emitent_map", "MasterDataEmitentMap.xlsx");
    }

    @Override
    public void callLoad(Connection conn) { concreteLoad(conn); }

    public StageMasterDataEmitentMapData readRow(Row row) {
        Iterator<Cell> cellIterator = row.iterator();
        String sourceSystemCode = null;
        String emitentCode = null;
        String emitentShortName = null;
        while (cellIterator.hasNext()) {
            Cell cell = cellIterator.next();
            switch (cell.getColumnIndex()) {
                case 0:
                    sourceSystemCode = cell.getStringCellValue();
                    break;
                case 1:
                    emitentCode = cell.getStringCellValue();
                    break;
                case 2:
                    emitentShortName = cell.getStringCellValue();
                    break;
                default:
                    throw new RuntimeException("Invalid column index");
            }
        }
        return new StageMasterDataEmitentMapData(sourceSystemCode, emitentCode, emitentShortName);
    }

    public void saveData(PreparedStatement stmtUpdate, List<? extends ExternalData> dataArray) {
        try {
            for(ExternalData iter : dataArray) {
                StageMasterDataEmitentMapData jter = (StageMasterDataEmitentMapData) iter;
                stmtUpdate.setLong(1, this.getFlowLoadId());
                stmtUpdate.setString(2, this.getFlowLogStartTimestamp().format(dateTimeFormat));
                stmtUpdate.setString(3, jter.sourceSystemCode);
                stmtUpdate.setString(4, jter.emitentCode);
                stmtUpdate.setString(5, jter.emitentShortName);
                stmtUpdate.addBatch();
            }
            setInsertCount(getInsertCount() + stmtUpdate.executeBatch().length);
            stmtUpdate.clearBatch();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

}
