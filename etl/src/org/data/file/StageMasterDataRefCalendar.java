package org.data.file;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.flow.Flow;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;

import static org.util.AuxUtil.*;

public class StageMasterDataRefCalendar extends org.data.FileEntity implements ExcelEntity {

    static class StageMasterDataRefCalendarData extends ExternalData {
        String fullDate;
        String holidayFlag;

        public StageMasterDataRefCalendarData(String fullDate,
                                    String holidayFlag) {
            this.fullDate = fullDate;
            this.holidayFlag = holidayFlag;
        }
    }

    public StageMasterDataRefCalendar(Flow flow) {
        super(flow, "master_data_ref_calendar", "MasterDataRefCalendar.xlsx");
    }

    @Override
    public void callLoad(Connection conn) {
        concreteLoad(conn);
    }

    public StageMasterDataRefCalendarData readRow(Row row) {
        Iterator<Cell> cellIterator = row.iterator();
        String fullDate = null;
        String holidayFlag = null;
        while (cellIterator.hasNext()) {
            Cell cell = cellIterator.next();
            switch (cell.getColumnIndex()) {
                case 0:
                    fullDate = cell.getLocalDateTimeCellValue().format(dateFormat);
                    break;
                case 1:
                    holidayFlag = cell.getStringCellValue();
                    break;
                default:
                    throw new RuntimeException("Invalid column index");
            }
        }
        return new StageMasterDataRefCalendarData(fullDate, holidayFlag);
    }

    public void saveData(PreparedStatement stmtUpdate, List<? extends ExternalData> dataArray) {
        try {
            for(ExternalData iter : dataArray) {
                StageMasterDataRefCalendarData jter = (StageMasterDataRefCalendarData) iter;
                stmtUpdate.setLong(1, this.getFlowLoadId());
                stmtUpdate.setString(2, this.getFlowLogStartTimestamp().format(dateTimeFormat));
                stmtUpdate.setString(3, jter.fullDate);
                stmtUpdate.setString(4, jter.holidayFlag);
                stmtUpdate.addBatch();
            }
            setInsertCount(getInsertCount() + stmtUpdate.executeBatch().length);
            stmtUpdate.clearBatch();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

}
