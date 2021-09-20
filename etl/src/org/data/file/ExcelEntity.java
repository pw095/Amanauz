package org.data.file;

import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.data.AbstractEntity;
import org.data.FileEntity;
import org.data.StageEntity;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public interface ExcelEntity extends StageEntity {

//    public abstract <T extends ExternalData> T readRow(Row row);
    public abstract List<? extends ExternalData> readRow(Row row);

    public abstract boolean ifCancelLoad(FileEntity.FileInfo fileInfo);
    public abstract String getFileName();
    public abstract void setFileName(String fileName);
//    public abstract List<String> getFileNameList();
    public abstract List<FileEntity.FileInfo> getFileInfo();
    @Override
    default void detailLoad(PreparedStatement stmt) {

        for (FileEntity.FileInfo fileInfo : getFileInfo()) {
            if (ifCancelLoad(fileInfo)) {
                continue;
            }
            setFileName(fileInfo.getFileName());
            System.out.println(fileInfo.getFileName());
            try (InputStream inputStream = new FileInputStream(fileInfo.getFilePathString())) {
                XSSFWorkbook workbook = new XSSFWorkbook(inputStream);
                for (Sheet rows : workbook) {
                    saveData(stmt, readWorkSheet(rows));
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    default List<? extends ExternalData> readWorkSheet(Sheet workSheet) {
        List<ExternalData> externalDataList = new ArrayList<>();
        for (Row row : workSheet) {
            if (row.getRowNum() == 0) {
                readHeader(row);
            } else if (row.getRowNum() > 0) {
                externalDataList.addAll(readRow(row));
            }
        }
        return externalDataList;
    }

    default void readHeader(Row row) { }

}
