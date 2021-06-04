package org.data;

import org.flow.Flow;
import org.json.JSONArray;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

import static org.util.AuxUtil.dateTimeFormat;

public class StageIndexSecurityWeight extends PeriodEntity implements ImoexSourceEntity {

    static class StageIndexSecurityWeightData extends ExternalData {
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

    public StageIndexSecurityWeight(Flow flow) {
        super(flow, "index_security_weight");
    }

    public void detailLoad(PreparedStatement stmtUpdate) {
        final String objectJSON = "analytics";
        final String urlStringRaw = "http://iss.moex.com/iss/statistics/engines/stock/markets/index/analytics/IMOEX.json" +
                                    "?iss.meta=off&iss.only=analytics&limit=100&analytics.columns=";
        final String urlColumnList = "indexid,tradedate,ticker,shortnames,secids,weight,tradingsession";

        final String urlStringData = urlStringRaw.concat(urlColumnList);
        saveData(stmtUpdate, periodMultiLoad(urlStringData, this, objectJSON));
    }

    @Override
    public StageIndexSecurityWeightData accumulateData(JSONArray jsonArray) {
        StageIndexSecurityWeightData data;
        data = new StageIndexSecurityWeightData(jsonArray.optString(0), // indexid
                                                jsonArray.optString(1), // tradedate
                                                jsonArray.optString(2), // ticker
                                                jsonArray.optString(3), // shortnames
                                                jsonArray.optString(4), // secids
                                                jsonArray.optDouble(5), // weight
                                                jsonArray.optInt(6)); // tradingsession
        return data;
    }

    @Override
    public void saveData(PreparedStatement stmtUpdate, List<? extends ExternalData> dataArray) {
        try {
            for(ExternalData iter : dataArray) {
                StageIndexSecurityWeightData jter = (StageIndexSecurityWeightData) iter;
                stmtUpdate.setLong(1, this.getFlowLoadId());
                stmtUpdate.setString(2, this.getFlowLogStartTimestamp().format(dateTimeFormat));
                stmtUpdate.setString(3, jter.indexId); // indexid
                stmtUpdate.setString(4, jter.tradeDate); // tradedate
                stmtUpdate.setString(5, jter.ticker); // ticker
                stmtUpdate.setString(6, jter.shortNames); // shortnames
                stmtUpdate.setString(7, jter.secIds); // secids
                stmtUpdate.setDouble(8, jter.weight); // weight
                stmtUpdate.setInt(9, jter.tradingSession); // tradingsession
                stmtUpdate.addBatch();
            }
            setInsertCount(getInsertCount() + stmtUpdate.executeBatch().length);
            stmtUpdate.clearBatch();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
