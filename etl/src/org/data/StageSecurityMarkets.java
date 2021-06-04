package org.data;

import org.flow.Flow;
import org.json.JSONArray;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

import static org.util.AuxUtil.dateTimeFormat;

public class StageSecurityMarkets extends SnapshotEntity implements ImoexSourceEntity {

    static class StageSecurityMarketsData extends ExternalData {
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

    public StageSecurityMarkets(Flow flow) {
        super(flow, "security_markets");
    }

    @Override
    public void detailLoad(PreparedStatement stmtUpdate) {
        final String objectJSON = "markets";
        String urlStringRaw = "http://iss.moex.com/iss/index.json?iss.meta=off&iss.only=markets&markets.columns=";
        String urlColumnList = "id,trade_engine_id,trade_engine_name,trade_engine_title,market_name," +
                               "market_title,market_id,marketplace";

        String urlStringData = urlStringRaw.concat(urlColumnList);
        load(stmtUpdate, urlStringData, objectJSON);
    }

    @Override
    public StageSecurityMarketsData accumulateData(JSONArray jsonArray) {
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

    @Override
    public void saveData(PreparedStatement stmtUpdate, List<? extends ExternalData> dataArray) {
        try {
            for(ExternalData iter : dataArray) {
                StageSecurityMarketsData jter = (StageSecurityMarketsData) iter;
                stmtUpdate.setLong(1, this.getFlowLoadId());
                stmtUpdate.setString(2, this.getFlowLogStartTimestamp().format(dateTimeFormat));
                stmtUpdate.setInt(3, jter.id);
                stmtUpdate.setInt(4, jter.tradeEngineId);
                stmtUpdate.setString(5, jter.tradeEngineName);
                stmtUpdate.setString(6, jter.tradeEngineTitle);
                stmtUpdate.setString(7, jter.marketName);
                stmtUpdate.setString(8, jter.marketTitle);
                stmtUpdate.setInt(9, jter.marketId);
                stmtUpdate.setString(10, jter.marketPlace);
                stmtUpdate.addBatch();
            }
            setInsertCount(getInsertCount() + stmtUpdate.executeBatch().length);
            stmtUpdate.clearBatch();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
