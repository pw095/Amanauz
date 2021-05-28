package org.data;

import org.json.JSONArray;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

public class StageIndexSecurityWeight extends ImoexWebSiteEntity {

    static class StageIndexSecurityWeightData extends ImoexData {
        String indexId;
        String tradeDate;
        String ticker;
        String shortNames;
        String secIds;
        double weight;
        int    tradingSession;

        public StageIndexSecurityWeightData(String indexId,
                                            String tradeDate,
                                            String ticker,
                                            String shortNames,
                                            String secIds,
                                            double weight,
                                            int    tradingSession) {
            this.indexId = indexId;
            this.tradeDate = tradeDate;
            this.ticker = ticker;
            this.shortNames = shortNames;
            this.secIds = secIds;
            this.weight = weight;
            this.tradingSession = tradingSession;
        }

    }

    public StageIndexSecurityWeight(long flowLoadId) {
        super(flowLoadId, "index_security_weight");
    }

    public void detailLoad(PreparedStatement stmtUpdate) {
        String urlStringRaw = "http://iss.moex.com/iss/statistics/engines/stock/markets/index/analytics/IMOEX.json?iss.meta=off&iss.only=analytics&limit=100";
        String urlStringData = urlStringRaw.concat("&analytics.columns=indexid,tradedate,ticker,shortnames,secids,weight,tradingsession");

        try {
            historyLoad(stmtUpdate, urlStringData, "analytics");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    StageIndexSecurityWeightData accumulateData(JSONArray jsonArray) {
        StageIndexSecurityWeightData data;
        data = new StageIndexSecurityWeightData(jsonArray.optString(0),
                                                jsonArray.optString(1),
                                                jsonArray.optString(2),
                                                jsonArray.optString(3),
                                                jsonArray.optString(4),
                                                jsonArray.optDouble(5),
                                                jsonArray.optInt(6));
//        System.out.println(data.tradeDate);
        return data;
    }

    void saveData(PreparedStatement stmtUpdate, List<? extends ImoexData> dataArray) {
        try {
            System.out.println("iter.count = " + dataArray.size());
        for(ImoexData iter : dataArray) {
            StageIndexSecurityWeightData jter = (StageIndexSecurityWeightData) iter;
//            System.out.println(jter.tradeDate);
                stmtUpdate.setString(1, jter.indexId);
                stmtUpdate.setString(2, jter.tradeDate);
                stmtUpdate.setString(3, jter.ticker);
                stmtUpdate.setString(4, jter.shortNames);
                stmtUpdate.setString(5, jter.secIds);
                stmtUpdate.setDouble(6, jter.weight);
                stmtUpdate.setInt(7, jter.tradingSession);
                stmtUpdate.setLong(8, this.getFlowLoadId());
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
