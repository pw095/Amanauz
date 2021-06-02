package org.data;

import org.json.JSONArray;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

public class StageSecurityBoardGroups extends ImoexWebSiteEntity {

    static class StageSecurityBoardGroupsData extends ImoexData {
        int    id;
        int    tradeEngineId;
        String tradeEngineName;
        String tradeEngineTitle;
        int    marketId;
        String marketName;
        String name;
        String title;
        int    isDefault;
        int    boardGroupId;
        int    isTraded;


        public StageSecurityBoardGroupsData(int    id,
                                            int    tradeEngineId,
                                            String tradeEngineName,
                                            String tradeEngineTitle,
                                            int    marketId,
                                            String marketName,
                                            String name,
                                            String title,
                                            int    isDefault,
                                            int    boardGroupId,
                                            int    isTraded) {
            this.id = id;
            this.tradeEngineId = tradeEngineId;
            this.tradeEngineName = tradeEngineName;
            this.tradeEngineTitle = tradeEngineTitle;
            this.marketId = marketId;
            this.marketName = marketName;
            this.name = name;
            this.title = title;
            this.isDefault = isDefault;
            this.boardGroupId = boardGroupId;
            this.isTraded = isTraded;
        }

    }

    public StageSecurityBoardGroups(long flowLoadId) {
        super(flowLoadId, "security_board_groups");
    }

    public void detailLoad(PreparedStatement stmtUpdate) {
        String urlStringData = "http://iss.moex.com/iss/index.json?iss.meta=off&iss.only=boardgroups&boardgroups.columns=id,trade_engine_id,trade_engine_name,trade_engine_title,market_id,market_name,name,title,is_default,board_group_id,is_traded";

        try {
//            completeLoad(stmtUpdate, null, urlStringData);
//            singleIterationLoad(stmtUpdate, urlStringData, "boardgroups");
            noHistoryLoad(stmtUpdate, urlStringData, "boardgroups", false);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    StageSecurityBoardGroupsData accumulateData(JSONArray jsonArray) {
        StageSecurityBoardGroupsData data;
        data = new StageSecurityBoardGroupsData(jsonArray.optInt(0), // id
                                                jsonArray.optInt(1), // trade_engine_id
                                                jsonArray.optString(2), // trade_engine_name
                                                jsonArray.optString(3), // trade_engine_title
                                                jsonArray.optInt(4), // market_id
                                                jsonArray.optString(5), // market_name
                                                jsonArray.optString(6),    // name
                                                jsonArray.optString(7),    // title
                                                jsonArray.optInt(8), // is_default
                                                jsonArray.optInt(9),    // board_group_id
                                                jsonArray.optInt(10));   // is_traded
        return data;
    }

    void saveData(PreparedStatement stmtUpdate, List<? extends ImoexData> dataArray) {
        try {
            System.out.println("iter.count = " + dataArray.size());
            for(ImoexData iter : dataArray) {
                StageSecurityBoardGroupsData jter = (StageSecurityBoardGroupsData) iter;
//                System.out.println("jter.marketName = " + jter.marketName);
                stmtUpdate.setInt(1, jter.id);
                stmtUpdate.setInt(2, jter.tradeEngineId);
                stmtUpdate.setString(3, jter.tradeEngineName);
                stmtUpdate.setString(4, jter.tradeEngineTitle);
                stmtUpdate.setInt(5, jter.marketId);
                stmtUpdate.setString(6, jter.marketName);
                stmtUpdate.setString(7, jter.name);
                stmtUpdate.setString(8, jter.title);
                stmtUpdate.setInt(9, jter.isDefault);
                stmtUpdate.setInt(10, jter.boardGroupId);
                stmtUpdate.setInt(11, jter.isTraded);
                stmtUpdate.setLong(12, this.getFlowLoadId());
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
