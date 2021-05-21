package org.data;

import org.json.JSONArray;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

public class StageIndexSecurityWeight extends ImoexWebSiteEntity {

    static class StageIndexSecurityWeightData extends ImoexData {
        String indexName;
        String tradeDate;
        String securityId;
        double weight;

        public StageIndexSecurityWeightData(String indexName, String tradeDate, String securityId, double weight) {
            this.indexName = indexName;
            this.tradeDate = tradeDate;
            this.securityId = securityId;
            this.weight = weight;
        }

    }

    public StageIndexSecurityWeight(long flowLoadId) {
        super(flowLoadId, "index_security_weight"/*, loadMode*/);
    }

    public void detailLoad(PreparedStatement stmtUpdate) {
        String urlStringRaw = "http://iss.moex.com/iss/statistics/engines/stock/markets/index/analytics/IMOEX.json?iss.meta=off&iss.only=analytics";
        String urlStringMeta = urlStringRaw.concat(".cursor&analytics.cursor.columns=TOTAL,PAGESIZE,NEXT_DATE");
        String urlStringData = urlStringRaw.concat("&analytics.columns=indexid,tradedate,secids,weight");

        try {
            completeLoad(stmtUpdate, urlStringMeta, urlStringData);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    StageIndexSecurityWeightData accumulateData(JSONArray jsonArray) {
        StageIndexSecurityWeightData data;
        data = new StageIndexSecurityWeightData(jsonArray.optString(0),
                                                jsonArray.optString(1),
                                                jsonArray.optString(2),
                                                jsonArray.optDouble(3));
//        System.out.println(data.tradeDate);
        return data;
    }

    void saveData(PreparedStatement stmtUpdate, List<? extends ImoexData> dataArray) {
        try {
            System.out.println("iter.count = " + dataArray.size());
        for(ImoexData iter : dataArray) {
            StageIndexSecurityWeightData jter = (StageIndexSecurityWeightData) iter;
//            System.out.println(jter.tradeDate);
                stmtUpdate.setString(1, jter.indexName);
                stmtUpdate.setString(2, jter.tradeDate);
                stmtUpdate.setString(3, jter.securityId);
                stmtUpdate.setDouble(4, jter.weight);
                stmtUpdate.setLong(5, this.getFlowLoadId());
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
