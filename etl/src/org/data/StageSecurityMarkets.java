package org.data;

import org.json.JSONArray;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

public class StageSecurityMarkets extends ImoexWebSiteEntity {

    static class StageSecurityMarketsData extends ImoexData {
        int    id;
        int    tradeEngineId;
        String tradeEngineName;
        String tradeEngineTitle;
        String marketName;
        String marketTitle;
        int    marketId;
        String marketPlace;


        public StageSecurityMarketsData(int    id,
                                        int    tradeEngineId,
                                        String tradeEngineName,
                                        String tradeEngineTitle,
                                        String marketName,
                                        String marketTitle,
                                        int    marketId,
                                        String marketPlace) {
            this.id = id;
            this.tradeEngineId = tradeEngineId;
            this.tradeEngineName = tradeEngineName;
            this.tradeEngineTitle = tradeEngineTitle;
            this.marketName = marketName;
            this.marketTitle = marketTitle;
            this.marketId = marketId;
            this.marketPlace = marketPlace;
        }

    }

    public StageSecurityMarkets(long flowLoadId) {
        super(flowLoadId, "security_markets");
    }

    public void detailLoad(PreparedStatement stmtUpdate) {
        String urlStringData = "http://iss.moex.com/iss/index.json?iss.meta=off&iss.only=markets&markets.columns=id,trade_engine_id,trade_engine_name,trade_engine_title,market_name,market_title,market_id,marketplace";

        try {
//            completeLoad(stmtUpdate, null, urlStringData);
//            singleIterationLoad(stmtUpdate, urlStringData, "markets");
            noHistoryLoad(stmtUpdate, urlStringData, "markets", false);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    StageSecurityMarketsData accumulateData(JSONArray jsonArray) {
        StageSecurityMarketsData data;
        data = new StageSecurityMarketsData(jsonArray.optInt(0), // id
                                            jsonArray.optInt(1), // trade_engine_id
                                            jsonArray.optString(2), // trade_engine_name
                                            jsonArray.optString(3), // trade_engine_title
                                            jsonArray.optString(4), // market_name
                                            jsonArray.optString(5), // market_title
                                            jsonArray.optInt(6),    // market_id
                                            jsonArray.optString(7));  // marketplace
        return data;
    }

    void saveData(PreparedStatement stmtUpdate, List<? extends ImoexData> dataArray) {
        try {
            System.out.println("iter.count = " + dataArray.size());
            for(ImoexData iter : dataArray) {
                StageSecurityMarketsData jter = (StageSecurityMarketsData) iter;
//                System.out.println("jter.marketName = " + jter.marketName);
                stmtUpdate.setInt(1, jter.id);
                stmtUpdate.setInt(2, jter.tradeEngineId);
                stmtUpdate.setString(3, jter.tradeEngineName);
                stmtUpdate.setString(4, jter.tradeEngineTitle);
                stmtUpdate.setString(5, jter.tradeEngineTitle);
                stmtUpdate.setString(6, jter.marketTitle);
                stmtUpdate.setInt(7, jter.marketId);
                stmtUpdate.setString(8, jter.marketPlace);
                stmtUpdate.setLong(9, this.getFlowLoadId());
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
