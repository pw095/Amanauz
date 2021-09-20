package org.data.file;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.flow.Flow;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.util.AuxUtil.dateTimeFormat;

public class StageDefaultDataSecurity extends org.data.FileEntity implements ExcelEntity {

    static class StageDefaultDataSecurityData extends ExternalData {
        String securityId;
        String securityName;

        public StageDefaultDataSecurityData(String securityId, String securityName) {
            this.securityId = securityId;
            this.securityName = securityName;
        }
    }

    public StageDefaultDataSecurity(Flow flow) {
        super(flow, "default_data_security", "DefaultDataSecurity.xlsx");
    }

    @Override
    public void callLoad(Connection conn) {
        concreteLoad(conn);
    }

    public List<StageDefaultDataSecurityData> readRow(Row row) {
        List<StageDefaultDataSecurityData> arrList = new ArrayList<>();
        arrList.add(readTuple(row));
        return arrList;
    }

    public StageDefaultDataSecurityData readTuple(Row row) {
        Iterator<Cell> cellIterator = row.iterator();
        String securityId = null;
        String securityName = null;
        while (cellIterator.hasNext()) {
            Cell cell = cellIterator.next();
            switch (cell.getColumnIndex()) {
                case 0:
                    securityId = cell.getStringCellValue();
                    break;
                case 1:
                    securityName = cell.getStringCellValue();
                    break;
                default:
                    throw new RuntimeException("Invalid column index");
            }
        }
        return new StageDefaultDataSecurityData(securityId, securityName);
    }

    public void saveData(PreparedStatement stmtUpdate, List<? extends ExternalData> dataArray) {
        try {
            for(ExternalData iter : dataArray) {
                StageDefaultDataSecurityData jter = (StageDefaultDataSecurityData) iter;
                stmtUpdate.setLong(1, this.getFlowLoadId());
                stmtUpdate.setString(2, this.getFlowLogStartTimestamp().format(dateTimeFormat));
                stmtUpdate.setString(3, jter.securityId);
                stmtUpdate.setString(4, jter.securityName);
                stmtUpdate.addBatch();
            }
            setInsertCount(getInsertCount() + stmtUpdate.executeBatch().length);
            stmtUpdate.clearBatch();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

}
