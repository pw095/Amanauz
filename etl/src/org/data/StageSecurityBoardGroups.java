package org.data;

import org.flow.Flow;
import org.json.JSONArray;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

import static org.util.AuxUtil.dateTimeFormat;

public class StageSecurityBoardGroups extends SnapshotEntity implements ImoexSourceEntity {

    static class StageSecurityBoardGroupsData extends ExternalData {
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

    public StageSecurityBoardGroups(Flow flow) {
        super(flow, "security_board_groups");
    }

    @Override
    public void detailLoad(PreparedStatement stmtUpdate) {
        final String objectJSON = "boardgroups";
        String urlStringRaw = "http://iss.moex.com/iss/index.json" +
                              "?iss.meta=off&iss.only=boardgroups&boardgroups.columns=";
        String urlColumnList = "id,trade_engine_id,trade_engine_name,trade_engine_title,market_id,market_name,name," +
                               "title,is_default,board_group_id,is_traded";

        String urlStringData = urlStringRaw.concat(urlColumnList);
        saveData(stmtUpdate, load(urlStringData, objectJSON));
    }

    @Override
    public StageSecurityBoardGroupsData accumulateData(JSONArray jsonArray) {
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

    @Override
    public void saveData(PreparedStatement stmtUpdate, List<? extends ExternalData> dataArray) {
        try {
            for(ExternalData iter : dataArray) {
                StageSecurityBoardGroupsData jter = (StageSecurityBoardGroupsData) iter;
                stmtUpdate.setLong(1, this.getFlowLoadId());
                stmtUpdate.setString(2, this.getFlowLogStartTimestamp().format(dateTimeFormat));
                stmtUpdate.setInt(3, jter.id);
                stmtUpdate.setInt(4, jter.tradeEngineId);
                stmtUpdate.setString(5, jter.tradeEngineName);
                stmtUpdate.setString(6, jter.tradeEngineTitle);
                stmtUpdate.setInt(7, jter.marketId);
                stmtUpdate.setString(8, jter.marketName);
                stmtUpdate.setString(9, jter.name);
                stmtUpdate.setString(10, jter.title);
                stmtUpdate.setInt(11, jter.isDefault);
                stmtUpdate.setInt(12, jter.boardGroupId);
                stmtUpdate.setInt(13, jter.isTraded);
                stmtUpdate.addBatch();
            }
            setInsertCount(getInsertCount() + stmtUpdate.executeBatch().length);
            stmtUpdate.clearBatch();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
