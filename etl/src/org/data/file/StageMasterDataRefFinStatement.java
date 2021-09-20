package org.data.file;

import org.apache.poi.ss.formula.BaseFormulaEvaluator;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellValue;
import org.apache.poi.ss.usermodel.FormulaEvaluator;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.xssf.usermodel.XSSFFormulaEvaluator;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.flow.Flow;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.util.AuxUtil.dateFormat;
import static org.util.AuxUtil.dateTimeFormat;

public class StageMasterDataRefFinStatement extends org.data.FileEntity implements ExcelEntity {

    static class StageMasterDataRefFinStatementData extends ExternalData {

        int hierLevel;
        String leafCode;
        String code;
        String parentLeafCode;
        String parentCode;
        String fullName;

        public StageMasterDataRefFinStatementData(int hierLevel,
                                                  String leafCode,
                                                  String code,
                                                  String parentLeafCode,
                                                  String parentCode,
                                                  String fullName) {
            this.hierLevel = hierLevel;
            this.leafCode = leafCode;
            this.code = code;
            this.parentLeafCode = parentLeafCode;
            this.parentCode = parentCode;
            this.fullName = fullName;
        }
    }

    public StageMasterDataRefFinStatement(Flow flow) {
        super(flow, "master_data_ref_fin_statement", "MasterDataRefFinStatement.xlsx");
    }

    @Override
    public void callLoad(Connection conn) {
        concreteLoad(conn);
    }

    public List<StageMasterDataRefFinStatementData> readRow(Row row) {
        List<StageMasterDataRefFinStatementData> arrList = new ArrayList<>();
        arrList.add(readTuple(row));
        return arrList;
    }

    public StageMasterDataRefFinStatementData readTuple(Row row) {
        Iterator<Cell> cellIterator = row.iterator();

        int hierLevel = -1;
        String leafCode = null;
        String code = null;
        String parentLeafCode = null;
        String parentCode = null;
        String fullName = null;

        XSSFFormulaEvaluator formulaEvaluator = new XSSFFormulaEvaluator((XSSFWorkbook) row.getSheet().getWorkbook());
//        formulaEvaluator.evaluateAll();
//        BaseFormulaEvaluator.evaluateAllFormulaCells(row.getSheet().getWorkbook());

        while (cellIterator.hasNext()) {
            Cell cell = cellIterator.next();
            switch (cell.getColumnIndex()) {
                case 0:
                    hierLevel = (int) cell.getNumericCellValue();
                    break;
                case 1:
                    leafCode = cell.getStringCellValue();
                    break;
                case 2:
                    switch (cell.getCellType()) {
                        case FORMULA:
                            code = formulaEvaluator.evaluate(cell).getStringValue();
                            break;
                        default:
                            code = cell.getStringCellValue();
                            break;
                    }
                    break;
                case 3:
                    parentLeafCode = cell.getStringCellValue();
                    break;
                case 4:
                    switch (cell.getCellType()) {
                        case FORMULA:
                            parentCode = formulaEvaluator.evaluate(cell).getStringValue();
                            break;
                        default:
                            parentCode = cell.getStringCellValue();
                            break;
                    }
                    break;
                case 5:
                    fullName = cell.getStringCellValue();
                    break;
                default:
                    throw new RuntimeException("Invalid column index");
            }
        }
        return new StageMasterDataRefFinStatementData(hierLevel, leafCode, code, parentLeafCode, parentCode, fullName);
    }

    public void saveData(PreparedStatement stmtUpdate, List<? extends ExternalData> dataArray) {
        try {
            for(ExternalData iter : dataArray) {
                StageMasterDataRefFinStatementData jter = (StageMasterDataRefFinStatementData) iter;
                stmtUpdate.setLong(1, this.getFlowLoadId());
                stmtUpdate.setString(2, this.getFlowLogStartTimestamp().format(dateTimeFormat));
                stmtUpdate.setInt(3, jter.hierLevel);
                stmtUpdate.setString(4, jter.leafCode);
                stmtUpdate.setString(5, jter.code);
                stmtUpdate.setString(6, jter.parentLeafCode);
                stmtUpdate.setString(7, jter.parentCode);
                stmtUpdate.setString(8, jter.fullName);
                stmtUpdate.addBatch();
            }
            setInsertCount(getInsertCount() + stmtUpdate.executeBatch().length);
            stmtUpdate.clearBatch();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

}
