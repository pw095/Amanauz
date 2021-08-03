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

public class StageMasterDataSecurityTypeMap extends org.data.FileEntity implements ExcelEntity {

    static class StageMasterDataSecurityTypeMapData extends ExternalData {
        String tableName;
        String typeId;
        String typeName;
        int securityTypeId;

        public StageMasterDataSecurityTypeMapData(String tableName,
                                                  String typeId,
                                                  String typeName,
                                                  int    securityTypeId) {
            this.tableName = tableName;
            this.typeId = typeId;
            this.typeName = typeName;
            this.securityTypeId = securityTypeId;
        }
    }

    public StageMasterDataSecurityTypeMap(Flow flow) {
        super(flow, "master_data_security_type_map", "MasterDataSecurityTypeMap.xlsx");
    }

    @Override
    public void callLoad(Connection conn) { concreteLoad(conn); }

    public StageMasterDataSecurityTypeMapData readRow(Row row) {
        Iterator<Cell> cellIterator = row.iterator();
        String tableName = null;
        String typeId = null;
        String typeName = null;
        int securityTypeId = 0;
        while (cellIterator.hasNext()) {
            Cell cell = cellIterator.next();
            switch (cell.getColumnIndex()) {
                case 0:
                    tableName = cell.getStringCellValue();
                    break;
                case 1:
                    switch (cell.getCellType()) {
                        case STRING:
                            typeId = cell.getStringCellValue();
                            break;
                        case NUMERIC:
                            typeId = Integer.valueOf( (int) cell.getNumericCellValue()).toString();
                            break;
                        default:
                            throw new RuntimeException("Invalid type_id cell type");
                    }
                    break;
                case 2:
                    typeName = cell.getStringCellValue();
                    break;
                case 3:
                    securityTypeId = (int) cell.getNumericCellValue();
                    break;
                default:
                    throw new RuntimeException("Invalid column index");
            }
        }
        return new StageMasterDataSecurityTypeMapData(tableName, typeId, typeName, securityTypeId);
    }

    public void saveData(PreparedStatement stmtUpdate, List<? extends ExternalData> dataArray) {
        try {
            for(ExternalData iter : dataArray) {
                StageMasterDataSecurityTypeMapData jter = (StageMasterDataSecurityTypeMapData) iter;
                stmtUpdate.setLong(1, this.getFlowLoadId());
                stmtUpdate.setString(2, this.getFlowLogStartTimestamp().format(dateTimeFormat));
                stmtUpdate.setString(3, jter.tableName);
                stmtUpdate.setString(4, jter.typeId);
                stmtUpdate.setString(5, jter.typeName);
                stmtUpdate.setInt(6, jter.securityTypeId);
                stmtUpdate.addBatch();
            }
            setInsertCount(getInsertCount() + stmtUpdate.executeBatch().length);
            stmtUpdate.clearBatch();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

}
