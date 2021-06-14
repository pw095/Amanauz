package org.data;

import org.flow.Flow;
import org.json.JSONArray;
import org.meta.MetaLayer;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

import static org.util.AuxUtil.dateTimeFormat;

public class StageSecurityRateShares extends PeriodEntity implements ImoexSourceEntity {

    static class StageSecurityRateSharesData extends ExternalData {
        String boardId;
        String tradeDate;
        String shortName;
        String securityId;
        double numTrades;
        double value;
        double open;
        double low;
        double high;
        double legalClosePrice;
        double waPrice;
        double close;
        double volume;
        double marketPrice2;
        double marketPrice3;
        double admittedQuote;
        double mp2ValTrd;
        double marketPrice3TradesValue;
        double admittedValue;
        double waVal;
        int    tradingSession;

        public StageSecurityRateSharesData(String boardId,
                                           String tradeDate,
                                           String shortName,
                                           String securityId,
                                           double numTrades,
                                           double value,
                                           double open,
                                           double low,
                                           double high,
                                           double legalClosePrice,
                                           double waPrice,
                                           double close,
                                           double volume,
                                           double marketPrice2,
                                           double marketPrice3,
                                           double admittedQuote,
                                           double mp2ValTrd,
                                           double marketPrice3TradesValue,
                                           double admittedValue,
                                           double waVal,
                                           int    tradingSession) {
            this.boardId = boardId;
            this.tradeDate = tradeDate;
            this.shortName = shortName;
            this.securityId = securityId;
            this.numTrades = numTrades;
            this.value = value;
            this.open = open;
            this.low = low;
            this.high = high;
            this.legalClosePrice = legalClosePrice;
            this.waPrice = waPrice;
            this.close = close;
            this.volume = volume;
            this.marketPrice2 = marketPrice2;
            this.marketPrice3 = marketPrice3;
            this.admittedQuote = admittedQuote;
            this.mp2ValTrd = mp2ValTrd;
            this.marketPrice3TradesValue = marketPrice3TradesValue;
            this.admittedValue = admittedValue;
            this.waVal = waVal;
            this.tradingSession = tradingSession;
        }

    }

    public StageSecurityRateShares(Flow flow) {
        super(flow, MetaLayer.STAGE, "security_rate_shares");
    }

    @Override
    public void callLoad(Connection conn) {
        concreteLoad(conn);
    }

    @Override
    public void detailLoad(PreparedStatement stmtUpdate) {
        final String objectJSON = "history";
        final String urlStringRaw = "http://iss.moex.com/iss/history/engines/stock/markets/shares/securities.json" +
                                    "?iss.meta=off&history.columns=";
        final String urlColumnList = "BOARDID,TRADEDATE,SHORTNAME,SECID,NUMTRADES,VALUE,OPEN,LOW,HIGH," +
                                     "LEGALCLOSEPRICE,WAPRICE,CLOSE,VOLUME,MARKETPRICE2,MARKETPRICE3,ADMITTEDQUOTE," +
                                     "MP2VALTRD,MARKETPRICE3TRADESVALUE,ADMITTEDVALUE,WAVAL,TRADINGSESSION";

        final String urlStringData = urlStringRaw.concat(urlColumnList);
        periodMultiLoad(stmtUpdate, urlStringData, this, objectJSON);
    }

    @Override
    public StageSecurityRateSharesData accumulateData(JSONArray jsonArray) {
        StageSecurityRateSharesData data;
        data = new StageSecurityRateSharesData(jsonArray.optString(0), // BOARDID
                                               jsonArray.optString(1),  // TRADEDATE
                                               jsonArray.optString(2), // SHORTNAME
                                               jsonArray.optString(3), // SECID
                                               jsonArray.optDouble(4), // NUMTRADES
                                               jsonArray.optDouble(5), // VALUE
                                               jsonArray.optDouble(6), // OPEN
                                               jsonArray.optDouble(7), // LOW
                                               jsonArray.optDouble(8), // HIGH
                                               jsonArray.optDouble(9), // LEGALCLOSEPRICE
                                               jsonArray.optDouble(10), // WAPRICE
                                               jsonArray.optDouble(11), // CLOSE
                                               jsonArray.optDouble(12), // VOLUME
                                               jsonArray.optDouble(13), // MARKETPRICE2
                                               jsonArray.optDouble(14), // MARKETPRICE3
                                               jsonArray.optDouble(15), // ADMITTEDQUOTE
                                               jsonArray.optDouble(16), // MP2VALTRD
                                               jsonArray.optDouble(17), // MARKETPRICE3TRADESVALUE
                                               jsonArray.optDouble(18), // ADMITTEDVALUE
                                               jsonArray.optDouble(19), // WAVAL
                                               jsonArray.optInt(20)); // TRADINGSESSION
        return data;
    }

    @Override
    public void saveData(PreparedStatement stmtUpdate, List<? extends ExternalData> dataArray) {
        try {
            for(ExternalData iter : dataArray) {
                StageSecurityRateSharesData jter = (StageSecurityRateSharesData) iter;
                stmtUpdate.setLong(1, this.getFlowLoadId());
                stmtUpdate.setString(2, this.getFlowLogStartTimestamp().format(dateTimeFormat));
                stmtUpdate.setString(3, jter.boardId);
                stmtUpdate.setString(4, jter.tradeDate);
                stmtUpdate.setString(5, jter.shortName);
                stmtUpdate.setString(6, jter.securityId);
                stmtUpdate.setDouble(7, jter.numTrades);
                stmtUpdate.setDouble(8, jter.value);
                stmtUpdate.setDouble(9, jter.open);
                stmtUpdate.setDouble(10, jter.low);
                stmtUpdate.setDouble(11, jter.high);
                stmtUpdate.setDouble(12, jter.legalClosePrice);
                stmtUpdate.setDouble(13, jter.waPrice);
                stmtUpdate.setDouble(14, jter.close);
                stmtUpdate.setDouble(15, jter.volume);
                stmtUpdate.setDouble(16, jter.marketPrice2);
                stmtUpdate.setDouble(17, jter.marketPrice3);
                stmtUpdate.setDouble(18, jter.admittedQuote);
                stmtUpdate.setDouble(19, jter.mp2ValTrd);
                stmtUpdate.setDouble(20, jter.marketPrice3TradesValue);
                stmtUpdate.setDouble(21, jter.admittedValue);
                stmtUpdate.setDouble(22, jter.waVal);
                stmtUpdate.setInt(23, jter.tradingSession);
                stmtUpdate.addBatch();
            }
            setInsertCount(getInsertCount() + stmtUpdate.executeBatch().length);
            stmtUpdate.clearBatch();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
