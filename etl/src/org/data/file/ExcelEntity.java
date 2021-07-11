package org.data.file;

import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.data.StageEntity;
import org.json.JSONArray;
import org.meta.Meta;
import org.w3c.dom.Document;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.time.format.DateTimeFormatter;
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

    default List<StageRefCalendar.StageRefCalendarData> readWorkSheet(Sheet workSheet) {
        List<StageRefCalendar.StageRefCalendarData> stageRefCalendarDataList = new ArrayList<>();
        for (Row row : workSheet) {
            if (row.getRowNum() > 0) {
                stageRefCalendarDataList.add(readRow(row));
            }
            System.out.println(row.getRowNum());
        }
        return stageRefCalendarDataList;
    }

}
