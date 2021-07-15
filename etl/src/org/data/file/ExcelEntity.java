package org.data.file;

import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.data.AbstractEntity;
import org.data.StageEntity;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.List;

public interface ExcelEntity extends StageEntity {

    public abstract <T extends ExternalData> T readRow(Row row);
    public abstract boolean ifCancelLoad();
    public abstract String getFileName();

    @Override
    default void detailLoad(PreparedStatement stmt) {
        if (ifCancelLoad()) {
            return;
        }

        try (InputStream inputStream = new FileInputStream(getFileName())) {
            XSSFWorkbook workbook = new XSSFWorkbook(inputStream);
            for (Sheet rows : workbook) {
                saveData(stmt, readWorkSheet(rows));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    default List<? extends ExternalData> readWorkSheet(Sheet workSheet) {
        List<ExternalData> externalDataList = new ArrayList<>();
        for (Row row : workSheet) {
            if (row.getRowNum() > 0) {
                externalDataList.add(readRow(row));
            }
            System.out.println(row.getRowNum());
        }
        return externalDataList;
    }

}
