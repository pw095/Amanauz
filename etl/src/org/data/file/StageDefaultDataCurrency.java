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

public class StageDefaultDataCurrency extends org.data.FileEntity implements ExcelEntity {

    static class StageDefaultDataCurrencyData extends ExternalData {
        String isoCharCode;
        String isoNumCode;
        String rusName;
        String engName;
        int nominal;

        public StageDefaultDataCurrencyData(String isoCharCode, String isoNumCode, String rusName, String engName, int nominal) {
            this.isoCharCode = isoCharCode;
            this.isoNumCode = isoNumCode;
            this.rusName = rusName;
            this.engName = engName;
            this.nominal = nominal;
        }
    }

    public StageDefaultDataCurrency(Flow flow) {
        super(flow, "default_data_currency", "DefaultDataCurrency.xlsx");
    }

    @Override
    public void callLoad(Connection conn) {
        concreteLoad(conn);
    }

    public List<StageDefaultDataCurrencyData> readRow(Row row) {
        List<StageDefaultDataCurrencyData> arrList = new ArrayList<>();
        arrList.add(readTuple(row));
        return arrList;
    }

    public StageDefaultDataCurrencyData readTuple(Row row) {
        Iterator<Cell> cellIterator = row.iterator();
        String isoCharCode = null;
        String isoNumCode = null;
        String rusName = null;
        String engName = null;
        int nominal = 0;
        while (cellIterator.hasNext()) {
            Cell cell = cellIterator.next();
            switch (cell.getColumnIndex()) {
                case 0:
                    isoCharCode = cell.getStringCellValue();
                    break;
                case 1:
                    isoNumCode = Integer.valueOf((int) cell.getNumericCellValue()).toString();
                    break;
                case 2:
                    rusName = cell.getStringCellValue();
                    break;
                case 3:
                    engName = cell.getStringCellValue();
                    break;
                case 4:
                    nominal = (int) cell.getNumericCellValue();
                    break;
                default:
                    throw new RuntimeException("Invalid column index");
            }
        }
        return new StageDefaultDataCurrencyData(isoCharCode, isoNumCode, rusName, engName, nominal);
    }

    public void saveData(PreparedStatement stmtUpdate, List<? extends ExternalData> dataArray) {
        try {
            for(ExternalData iter : dataArray) {
                StageDefaultDataCurrencyData jter = (StageDefaultDataCurrencyData) iter;
                stmtUpdate.setLong(1, this.getFlowLoadId());
                stmtUpdate.setString(2, this.getFlowLogStartTimestamp().format(dateTimeFormat));
                stmtUpdate.setString(3, jter.isoCharCode);
                stmtUpdate.setString(4, jter.isoNumCode);
                stmtUpdate.setString(5, jter.rusName);
                stmtUpdate.setString(6, jter.engName);
                stmtUpdate.setInt(7, jter.nominal);
                stmtUpdate.addBatch();
            }
            setInsertCount(getInsertCount() + stmtUpdate.executeBatch().length);
            stmtUpdate.clearBatch();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

}
