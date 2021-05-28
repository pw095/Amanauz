package org.data;

import org.json.JSONArray;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

public class StageSecuritySharesRate extends ImoexWebSiteEntity {

    static class StageSecuritySharesRateData extends ImoexData {
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

        public StageSecuritySharesRateData(String boardId,
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

    public StageSecuritySharesRate(long flowLoadId) {
        super(flowLoadId, "security_shares_rate"/*, loadMode*/);
    }

    public void detailLoad(PreparedStatement stmtUpdate) {
//        &securities.columns=id,secid,shortname,regnumber,name,isin,is_traded,emitent_id,emitent_title,emitent_inn,emitent_okpo,gosreg,type,group,primary_boardid,marketprice_boardid&start=100
//        String urlStringRaw = "http://iss.moex.com/iss/securities.json?iss.meta=off&engine=stock&market=";
        String urlStringRaw = "http://iss.moex.com/iss/history/engines/stock/markets/";
        String urlStringData;
        String urlColumnList = "/securities.json?iss.meta=off&history.columns=BOARDID,TRADEDATE,SHORTNAME,SECID,NUMTRADES,VALUE,LOW,HIGH,LEGALCLOSEPRICE,WAPRICE,CLOSE,VOLUME,MARKETPRICE2,MARKETPRICE3,ADMITTEDQUOTE,MP2VALTRD,MARKETPRICE3TRADESVALUE,ADMITTEDVALUE,WAVAL,TRADINGSESSION";
        String urlStringMeta = null;
        urlStringData = urlStringRaw.concat("shares").concat(urlColumnList);

        try {
            historyLoad(stmtUpdate, urlStringData, "history");
//            completeLoad(stmtUpdate, urlStringMeta, urlStringData);
        } catch (Exception e) {
            e.printStackTrace();
        }

/*        urlStringData = urlStringRaw.concat("bonds").concat(urlColumnList);

        try {
            completeLoad(stmtUpdate, urlStringMeta, urlStringData);
        } catch (Exception e) {
            e.printStackTrace();
        }*/
    }

    StageSecuritySharesRateData accumulateData(JSONArray jsonArray) {
        StageSecuritySharesRateData data;
        data = new StageSecuritySharesRateData(jsonArray.optString(0), // board_id
                jsonArray.optString(1),  // trade_date
                jsonArray.optString(2), // short_name
                jsonArray.optString(3), // security_id
                jsonArray.optDouble(4), // num_trades
                jsonArray.optDouble(5), // value
                jsonArray.optDouble(6), // open
                jsonArray.optDouble(7), // low
                jsonArray.optDouble(8), // high
                jsonArray.optDouble(9), // legal_close_price
                jsonArray.optDouble(10), // wa_price
                jsonArray.optDouble(11), // close
                jsonArray.optDouble(12), // volume
                jsonArray.optDouble(13), // market_price_2
                jsonArray.optDouble(14), // market_price_3
                jsonArray.optDouble(15), // admitted_quote
                jsonArray.optDouble(16), // mp2_val_trd
                jsonArray.optDouble(17), // market_price_3_trades_value
                jsonArray.optDouble(18), // admitted_value
                jsonArray.optDouble(19), // wa_val
                jsonArray.optInt(20)); // trading_session
//        System.out.println(data.tradeDate);
        return data;
    }

    void saveData(PreparedStatement stmtUpdate, List<? extends ImoexData> dataArray) {
        try {
            System.out.println("iter.count = " + dataArray.size());
            for(ImoexData iter : dataArray) {
                StageSecuritySharesRateData jter = (StageSecuritySharesRateData) iter;
//            System.out.println(jter.tradeDate);
                stmtUpdate.setString(1, jter.boardId);
                stmtUpdate.setString(2, jter.tradeDate);
                stmtUpdate.setString(3, jter.shortName);
                stmtUpdate.setString(4, jter.securityId);
                stmtUpdate.setDouble(5, jter.numTrades);
                stmtUpdate.setDouble(6, jter.value);
                stmtUpdate.setDouble(7, jter.open);
                stmtUpdate.setDouble(8, jter.low);
                stmtUpdate.setDouble(9, jter.high);
                stmtUpdate.setDouble(10, jter.legalClosePrice);
                stmtUpdate.setDouble(11, jter.waPrice);
                stmtUpdate.setDouble(12, jter.close);
                stmtUpdate.setDouble(13, jter.volume);
                stmtUpdate.setDouble(14, jter.marketPrice2);
                stmtUpdate.setDouble(15, jter.marketPrice3);
                stmtUpdate.setDouble(16, jter.admittedQuote);
                stmtUpdate.setDouble(17, jter.mp2ValTrd);
                stmtUpdate.setDouble(18, jter.marketPrice3TradesValue);
                stmtUpdate.setDouble(19, jter.admittedValue);
                stmtUpdate.setDouble(20, jter.waVal);
                stmtUpdate.setInt(21, jter.tradingSession);
                stmtUpdate.setLong(22, this.getFlowLoadId());
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
