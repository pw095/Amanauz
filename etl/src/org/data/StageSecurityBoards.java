package org.data;

import org.flow.Flow;
import org.json.JSONArray;
import org.meta.MetaLayer;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

import static org.util.AuxUtil.dateTimeFormat;

public class StageSecurityBoards extends SnapshotEntity implements ImoexSourceEntity {

    static class StageSecurityBoardsData extends ExternalData {
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

    public StageSecurityBoards(Flow flow) {
        super(flow, MetaLayer.STAGE, "security_boards");
    }

    @Override
    public void callLoad(Connection conn) {
        concreteLoad(conn);
    }

    @Override
    public void detailLoad(PreparedStatement stmtUpdate) {
        final String objectJSON = "boards";
        String urlStringRaw = "http://iss.moex.com/iss/index.json?iss.meta=off&iss.only=boards&markets.columns=";
        String urlColumnList = "id,board_group_id,engine_id,market_id,boardid,board_title,is_traded,has_candles," +
                               "is_primary";

        String urlStringData = urlStringRaw.concat(urlColumnList);
        load(stmtUpdate, urlStringData, objectJSON);
    }

    @Override
    public StageSecurityBoardsData accumulateData(JSONArray jsonArray) {
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

    @Override
    public void saveData(PreparedStatement stmtUpdate, List<? extends ExternalData> dataArray) {
        try {
            for(ExternalData iter : dataArray) {
                StageSecurityBoardsData jter = (StageSecurityBoardsData) iter;
                stmtUpdate.setLong(1, this.getFlowLoadId());
                stmtUpdate.setString(2, this.getFlowLogStartTimestamp().format(dateTimeFormat));
                stmtUpdate.setInt(3, jter.id);
                stmtUpdate.setInt(4, jter.boardGroupId);
                stmtUpdate.setInt(5, jter.engineId);
                stmtUpdate.setInt(6, jter.marketId);
                stmtUpdate.setString(7, jter.boardId);
                stmtUpdate.setString(8, jter.boardTitle);
                stmtUpdate.setInt(9, jter.isTraded);
                stmtUpdate.setInt(10, jter.hasCandles);
                stmtUpdate.setInt(11, jter.isPrimary);
                stmtUpdate.addBatch();
            }
            setInsertCount(getInsertCount() + stmtUpdate.executeBatch().length);
            stmtUpdate.clearBatch();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
