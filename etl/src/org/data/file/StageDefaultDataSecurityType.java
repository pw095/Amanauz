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

public class StageDefaultDataSecurityType extends org.data.FileEntity implements ExcelEntity {

    static class StageDefaultDataSecurityTypeData extends ExternalData {
        String securityTypeId;
        String securityTypeName;

        public StageDefaultDataSecurityTypeData(String securityTypeId, String securityTypeName) {
            this.securityTypeId = securityTypeId;
            this.securityTypeName = securityTypeName;
        }
    }

    public StageDefaultDataSecurityType(Flow flow) {
        super(flow, "default_data_security_type", "DefaultDataSecurityType.xlsx");
    }

    @Override
    public void callLoad(Connection conn) {
        concreteLoad(conn);
    }

    public List<StageDefaultDataSecurityTypeData> readRow(Row row) {
        List<StageDefaultDataSecurityTypeData> arrList = new ArrayList<>();
        arrList.add(readTuple(row));
        return arrList;
    }

    public StageDefaultDataSecurityTypeData readTuple(Row row) {
        Iterator<Cell> cellIterator = row.iterator();
        String securityTypeId = null;
        String securityTypeName = null;
        while (cellIterator.hasNext()) {
            Cell cell = cellIterator.next();
            switch (cell.getColumnIndex()) {
                case 0:
                    securityTypeId = cell.getStringCellValue();
                    break;
                case 1:
                    securityTypeName = cell.getStringCellValue();
                    break;
                default:
                    throw new RuntimeException("Invalid column index");
            }
        }
        return new StageDefaultDataSecurityTypeData(securityTypeId, securityTypeName);
    }

    public void saveData(PreparedStatement stmtUpdate, List<? extends ExternalData> dataArray) {
        try {
            for(ExternalData iter : dataArray) {
                StageDefaultDataSecurityTypeData jter = (StageDefaultDataSecurityTypeData) iter;
                stmtUpdate.setLong(1, this.getFlowLoadId());
                stmtUpdate.setString(2, this.getFlowLogStartTimestamp().format(dateTimeFormat));
                stmtUpdate.setString(3, jter.securityTypeId);
                stmtUpdate.setString(4, jter.securityTypeName);
                stmtUpdate.addBatch();
            }
            setInsertCount(getInsertCount() + stmtUpdate.executeBatch().length);
            stmtUpdate.clearBatch();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

}
