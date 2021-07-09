package org.data.file;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.data.AbstractEntity;
import org.data.StageEntity;
import org.flow.Flow;
import org.meta.Meta;
import org.meta.MetaLayer;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.meta.Properties.getInstance;
import static org.util.AuxUtil.*;

public class StageRefCalendar extends AbstractEntity implements StageEntity {

    private final String fileName;
    private String fileHash;
    private long fileSize;
    private Path file;

    public String getFileName() {
        return this.fileName;
    }
    public String getFileHash() {
        return this.fileHash;
    }
    public long getFileSize() {
        return this.fileSize;
    }

    static class StageRefCalendarData extends ExternalData {
        String fullDate;
        String weekDayFlag;

        public StageRefCalendarData(String fullDate,
                                    String weekDayFlag) {
            this.fullDate = fullDate;
            this.weekDayFlag = weekDayFlag;
        }
    }

    public StageRefCalendar(Flow flow) {
        super(flow, MetaLayer.STAGE, "ref_calendar");
        fileName = getInstance().getProperty("fileDirectory") + "MasterDataRefCalendar.xlsx";
        file = Paths.get(fileName);
        try (InputStream inputStream = new FileInputStream(fileName)) {
            fileSize = Files.size(file);
            fileHash = DigestUtils.sha1Hex(inputStream);
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    @Override
    public void callLoad(Connection conn) {
        concreteLoad(conn);
    }

    @Override
    public void detailLoad(PreparedStatement stmt) {
        if (fileHash.equals(Meta.getPreviousFileHash(this))) {
            return;
        }

        try (InputStream inputStream = new FileInputStream(fileName)) {
            XSSFWorkbook workbook = new XSSFWorkbook(inputStream);
            for (Sheet rows : workbook) {
                saveData(stmt, readWorkSheet(rows));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private List<StageRefCalendarData> readWorkSheet(Sheet workSheet) {
        List<StageRefCalendarData> stageRefCalendarDataList = new ArrayList<>();
        for (Row row : workSheet) {
            if (row.getRowNum() > 0) {
                stageRefCalendarDataList.add(readRow(row));
            }
            System.out.println(row.getRowNum());
        }
        return stageRefCalendarDataList;
    }

    private StageRefCalendarData readRow(Row row) {
        Iterator<Cell> cellIterator = row.iterator();
        String fullDate = null;
        String weekDayFlag = null;
        while (cellIterator.hasNext()) {
            Cell cell = cellIterator.next();
            switch (cell.getColumnIndex()) {
                case 0:
                    fullDate = cell.getLocalDateTimeCellValue().format(dateFormat);
                    break;
                case 1:
                    weekDayFlag = cell.getStringCellValue();
                    break;
                default:
                    throw new RuntimeException("Invalid column index");
            }
        }
        return new StageRefCalendarData(fullDate, weekDayFlag);
    }

    public void saveData(PreparedStatement stmtUpdate, List<? extends ExternalData> dataArray) {
        try {
            for(ExternalData iter : dataArray) {
                StageRefCalendarData jter = (StageRefCalendarData) iter;
                stmtUpdate.setLong(1, this.getFlowLoadId());
                stmtUpdate.setString(2, this.getFlowLogStartTimestamp().format(dateTimeFormat));
                stmtUpdate.setString(3, jter.fullDate);
                stmtUpdate.setString(4, jter.weekDayFlag);
                stmtUpdate.addBatch();
            }
            setInsertCount(getInsertCount() + stmtUpdate.executeBatch().length);
            stmtUpdate.clearBatch();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

}
