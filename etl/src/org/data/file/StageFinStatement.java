package org.data.file;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.xssf.usermodel.XSSFFormulaEvaluator;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.flow.Flow;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.Month;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import static org.meta.Properties.getInstance;
import static org.util.AuxUtil.dateFormat;
import static org.util.AuxUtil.dateTimeFormat;

public class StageFinStatement extends org.data.FileEntity {

    static class StageFinStatementData extends ExternalData {

        String currency;
        String reportDate;
        String finStmtCode;
        String finStmtName;
        double value;

        public StageFinStatementData(String currency,
                                     String reportDate,
                                     String finStmtCode,
                                     String finStmtName,
                                     double value) {
            this.currency = currency;
            this.reportDate = reportDate;
            this.finStmtCode = finStmtCode;
            this.finStmtName = finStmtName;
            this.value = value;
        }
    }

    public Path getDirectoryPath() {
        System.out.println(getInstance().getProperty("finStatementFileDirectory"));
        System.out.println(Paths.get(getInstance().getProperty("finStatementFileDirectory")).normalize().toString());
        return Paths.get(getInstance().getProperty("finStatementFileDirectory")).normalize();
    }

    public StageFinStatement(Flow flow) {
        super(flow, "emitent_fin_statement", "");
    }

    HashMap<String, String> headerProperties;

    @Override
    public void callLoad(Connection conn) {
        concreteLoad(conn);
    }

    @Override
    public void readHeader(Row row) {
        XSSFFormulaEvaluator formulaEvaluator = new XSSFFormulaEvaluator((XSSFWorkbook) row.getSheet().getWorkbook());
        Iterator<Cell> cellIterator = row.iterator();
        HashMap<String, String> lReturn = new HashMap<>();
        while (cellIterator.hasNext()) {
            Cell cell = cellIterator.next();
            switch (cell.getColumnIndex()) {
                case 0:
                case 2:
                    break;
                case 1:
                    if (cell.getCellType() == CellType.STRING) {
                        String value = cell.getStringCellValue();

                        if (value.contains("млн.")) {
                            lReturn.put("Multiplier", "1000000");
                        } else if (value.contains("тыс.")) {
                            lReturn.put("Multiplier", "1000");
                        }

                        if (value.contains("руб.")) {
                            lReturn.put("Currency", "RUB");
                        }
                    } else {
                        throw new RuntimeException("Invalid header data type: currency and multiplier");
                    }
                    break;
                default:
                    int year;
                    switch (cell.getCellType()) {
                        case FORMULA:
                            year = (int) formulaEvaluator.evaluate(cell).getNumberValue();
                            break;
                        case NUMERIC:
                            year = (int) cell.getNumericCellValue();
                            break;
                        default:
                            throw new RuntimeException("Invalid year: " + cell.getCellType());
                    }
                    lReturn.put(Integer.valueOf(cell.getColumnIndex()).toString(),
                            LocalDate.of(year, Month.DECEMBER, 31).format(dateFormat));
            }
        }
        headerProperties = lReturn;
    }

    public List<StageFinStatementData> readRow(Row row) {
        Iterator<Cell> cellIterator = row.iterator();

        String currency = headerProperties.get("Currency");
        int multiplier = Integer.valueOf(headerProperties.get("Multiplier"));
        String finStmtCode = null;
        String finStmtName = null;
        double value;
        ArrayList<StageFinStatementData> arrList = new ArrayList<>();

        while (cellIterator.hasNext()) {

            Cell cell = cellIterator.next();
            PARENT_SWITCH: switch (cell.getColumnIndex()) {
                case 0:
                    switch (cell.getCellType()) {
                        case STRING:
                            finStmtCode = cell.getStringCellValue();
                            break;
                        default:
                            throw new RuntimeException("Invalid fin statement code!");
                    }
                    break;
                case 1:
                    switch (cell.getCellType()) {
                        case STRING:
                            finStmtName = cell.getStringCellValue();
                            break;
                        case BLANK:
                            throw new RuntimeException("Blank fin statement name!");
                        default:
                            throw new RuntimeException("Invalid fin statement name!");
                    }
                    break;
                case 2:
                    break;
                default:
                    switch (cell.getCellType()) {
                        case NUMERIC:
                            value = cell.getNumericCellValue() * ((double) multiplier);
                            break;
                        case BLANK:
                            break PARENT_SWITCH;
                        default:
                            throw new RuntimeException("Invalid value: " + cell.getCellType());
                    }
                    if (finStmtCode != null) {
                        arrList.add(new StageFinStatementData(currency,
                                    headerProperties.get(Integer.valueOf(cell.getColumnIndex()).toString()),
                                    finStmtCode,
                                    finStmtName,
                                    value));
                    }
            }
        }
        return arrList;
    }

    public void saveData(PreparedStatement stmtUpdate, List<? extends ExternalData> dataArray) {
        try {
            for(ExternalData iter : dataArray) {
                StageFinStatementData jter = (StageFinStatementData) iter;
                stmtUpdate.setLong(1, this.getFlowLoadId());
                stmtUpdate.setString(2, this.getFlowLogStartTimestamp().format(dateTimeFormat));
                stmtUpdate.setString(3, this.getFileName());
                stmtUpdate.setString(4, jter.currency);
                stmtUpdate.setString(5, jter.reportDate);
                stmtUpdate.setString(6, jter.finStmtCode);
                stmtUpdate.setString(7, jter.finStmtName);
                stmtUpdate.setDouble(8, Math.round(jter.value));
                stmtUpdate.addBatch();
            }
            setInsertCount(getInsertCount() + stmtUpdate.executeBatch().length);
            stmtUpdate.clearBatch();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

}
