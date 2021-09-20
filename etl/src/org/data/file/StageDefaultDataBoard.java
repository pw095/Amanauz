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

public class StageDefaultDataBoard extends org.data.FileEntity {

    static class StageDefaultDataBoardData extends ExternalData {
        String boardId;
        String boardTitle;

        public StageDefaultDataBoardData(String boardId, String boardTitle) {
            this.boardId = boardId;
            this.boardTitle = boardTitle;
        }
    }

    public StageDefaultDataBoard(Flow flow) {
        super(flow, "default_data_board", "DefaultDataBoard.xlsx");
    }

    @Override
    public void callLoad(Connection conn) {
        concreteLoad(conn);
    }

    public List<StageDefaultDataBoardData> readRow(Row row) {
        List<StageDefaultDataBoardData> arrList = new ArrayList<>();
        arrList.add(readTuple(row));
        return arrList;
    }

    public StageDefaultDataBoardData readTuple(Row row) {
        Iterator<Cell> cellIterator = row.iterator();
        String boardId = null;
        String boardTitle = null;
        while (cellIterator.hasNext()) {
            Cell cell = cellIterator.next();
            switch (cell.getColumnIndex()) {
                case 0:
                    boardId = cell.getStringCellValue();
                    break;
                case 1:
                    boardTitle = cell.getStringCellValue();
                    break;
                default:
                    throw new RuntimeException("Invalid column index");
            }
        }
        return new StageDefaultDataBoardData(boardId, boardTitle);
    }

    public void saveData(PreparedStatement stmtUpdate, List<? extends ExternalData> dataArray) {
        try {
            for(ExternalData iter : dataArray) {
                StageDefaultDataBoardData jter = (StageDefaultDataBoardData) iter;
                stmtUpdate.setLong(1, this.getFlowLoadId());
                stmtUpdate.setString(2, this.getFlowLogStartTimestamp().format(dateTimeFormat));
                stmtUpdate.setString(3, jter.boardId);
                stmtUpdate.setString(4, jter.boardTitle);
                stmtUpdate.addBatch();
            }
            setInsertCount(getInsertCount() + stmtUpdate.executeBatch().length);
            stmtUpdate.clearBatch();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

}
