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

import static org.util.AuxUtil.dateFormat;
import static org.util.AuxUtil.dateTimeFormat;

public class StageMasterDataEmitent extends org.data.FileEntity implements ExcelEntity {

    static class StageMasterDataEmitentData extends ExternalData {
        String fullName;
        String shortName;
        String regDate;
        String ogrn;
        String inn;

        public StageMasterDataEmitentData(String fullName,
                                          String shortName,
                                          String regDate,
                                          String ogrn,
                                          String inn) {
            this.fullName = fullName;
            this.shortName = shortName;
            this.regDate = regDate;
            this.ogrn = ogrn;
            this.inn = inn;
        }
    }

    public StageMasterDataEmitent(Flow flow) {
        super(flow, "master_data_emitent", "MasterDataEmitent.xlsx");
    }

    @Override
    public void callLoad(Connection conn) {
        concreteLoad(conn);
    }

    public List<StageMasterDataEmitentData> readRow(Row row) {
        List<StageMasterDataEmitentData> arrList = new ArrayList<>();
        arrList.add(readTuple(row));
        return arrList;
    }

    public StageMasterDataEmitentData readTuple(Row row) {
        Iterator<Cell> cellIterator = row.iterator();
        String fullName = null;
        String shortName = null;
        String regDate = null;
        String ogrn = null;
        String inn = null;
        while (cellIterator.hasNext()) {
            Cell cell = cellIterator.next();
            switch (cell.getColumnIndex()) {
                case 0:
                    fullName = cell.getStringCellValue();
                    break;
                case 1:
                    shortName = cell.getStringCellValue();
                    break;
                case 2:
                    switch (cell.getCellType()) {
                        case NUMERIC:
                            regDate = cell.getLocalDateTimeCellValue().format(dateFormat);
                            break;
                        case BLANK:
                            break;
                        default:
                            throw new RuntimeException("Invalid regDate type!");
                    }
                    break;
                case 3:
                    switch (cell.getCellType()) {
                        case STRING:
                            ogrn = cell.getStringCellValue();
                            break;
                        case NUMERIC:
                            ogrn = Long.valueOf( Math.round(cell.getNumericCellValue())).toString();
                            break;
                        case BLANK:
                            break;
                        default:
                            throw new RuntimeException("Invalid ogrn type!");
                    }
                    break;
                case 4:
                    switch (cell.getCellType()) {
                        case STRING:
                            inn = cell.getStringCellValue();
                            break;
                        case NUMERIC:
                            inn = Long.valueOf( Math.round(cell.getNumericCellValue())).toString();
                            break;
                        case BLANK:
                            break;
                        default:
                            throw new RuntimeException("Invalid inn type: " + cell.getCellType());
                    }
                    break;
                default:
                    throw new RuntimeException("Invalid column index");
            }
        }
        return new StageMasterDataEmitentData(fullName, shortName, regDate, ogrn, inn);
    }

    public void saveData(PreparedStatement stmtUpdate, List<? extends ExternalData> dataArray) {
        try {
            for(ExternalData iter : dataArray) {
                StageMasterDataEmitentData jter = (StageMasterDataEmitentData) iter;
                stmtUpdate.setLong(1, this.getFlowLoadId());
                stmtUpdate.setString(2, this.getFlowLogStartTimestamp().format(dateTimeFormat));
                stmtUpdate.setString(3, jter.fullName);
                stmtUpdate.setString(4, jter.shortName);
                stmtUpdate.setString(5, jter.regDate);
                stmtUpdate.setString(6, jter.ogrn);
                stmtUpdate.setString(7, jter.inn);
                stmtUpdate.addBatch();
            }
            setInsertCount(getInsertCount() + stmtUpdate.executeBatch().length);
            stmtUpdate.clearBatch();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

}
