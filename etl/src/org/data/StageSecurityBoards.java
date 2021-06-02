package org.data;

import org.json.JSONArray;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

public class StageSecurityBoards extends ImoexWebSiteEntity {

    static class StageSecurityBoardsData extends ImoexData {
        int    id;

        int    boardGroupId;
        int    engineId;
        int    marketId;
        String boardId;
        String boardTitle;
        int    isTraded;
        int    hasCandles;
        int    isPrimary;


        public StageSecurityBoardsData(int    id,
                                      int    boardGroupId,
                                      int    engineId,
                                      int    marketId,
                                      String boardId,
                                      String boardTitle,
                                      int    isTraded,
                                      int    hasCandles,
                                      int    isPrimary) {
            this.id = id;
            this.boardGroupId = boardGroupId;
            this.engineId = engineId;
            this.marketId = marketId;
            this.boardId = boardId;
            this.boardTitle = boardTitle;
            this.isTraded = isTraded;
            this.hasCandles = hasCandles;
            this.isPrimary = isPrimary;
        }

    }

    public StageSecurityBoards(long flowLoadId) {
        super(flowLoadId, "security_boards");
    }

    public void detailLoad(PreparedStatement stmtUpdate) {
        String urlStringData = "http://iss.moex.com/iss/index.json?iss.meta=off&iss.only=boards&markets.columns=id,board_group_id,engine_id,market_id,boardid,board_title,is_traded,has_candles,is_primary";

        try {
//            completeLoad(stmtUpdate, null, urlStringData);
//            singleIterationLoad(stmtUpdate, urlStringData, "boards");
            noHistoryLoad(stmtUpdate, urlStringData, "boards", false);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    StageSecurityBoardsData accumulateData(JSONArray jsonArray) {
        StageSecurityBoardsData data;
        data = new StageSecurityBoardsData(jsonArray.optInt(0), // id
                jsonArray.optInt(1), // board_group_id
                jsonArray.optInt(2), // engine_id
                jsonArray.optInt(3), // market_id
                jsonArray.optString(4), // boardid
                jsonArray.optString(5), // board_title
                jsonArray.optInt(6),    // is_traded
                jsonArray.optInt(7),    // has_candles
                jsonArray.getInt(8));  // is_primary
        return data;
    }

    void saveData(PreparedStatement stmtUpdate, List<? extends ImoexData> dataArray) {
        try {
            System.out.println("iter.count = " + dataArray.size());
            for(ImoexData iter : dataArray) {
                StageSecurityBoardsData jter = (StageSecurityBoardsData) iter;
//                System.out.println("jter.marketName = " + jter.marketName);
                stmtUpdate.setInt(1, jter.id);
                stmtUpdate.setInt(2, jter.boardGroupId);
                stmtUpdate.setInt(3, jter.engineId);
                stmtUpdate.setInt(4, jter.marketId);
                stmtUpdate.setString(5, jter.boardId);
                stmtUpdate.setString(6, jter.boardTitle);
                stmtUpdate.setInt(7, jter.isTraded);
                stmtUpdate.setInt(8, jter.hasCandles);
                stmtUpdate.setInt(9, jter.isPrimary);
                stmtUpdate.setLong(10, this.getFlowLoadId());
                stmtUpdate.addBatch();
            }
            setInsertCount(getInsertCount() + stmtUpdate.executeBatch().length);
//            System.out.println("Length: " + stmtUpdate.executeBatch().length);
            stmtUpdate.clearBatch();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
