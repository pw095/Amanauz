package org.data;

import org.flow.Flow;
import org.json.JSONArray;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

import static org.util.AuxUtil.dateTimeFormat;

public class StageSecurityRateBonds extends PeriodEntity implements ImoexSourceEntity {

    static class StageSecurityRateSharesData extends ExternalData {
        String boardId;
        String tradeDate;
        String shortName;
        String securityId;
        double numTrades;
        double value;
        double low;
        double high;
        double close;
        double legalClosePrice;
        double accInt;
        double waPrice;
        double yieldClose;
        double open;
        double volume;
        double marketPrice2;
        double marketPrice3;
        double admittedQuote;
        double mp2ValTrd;
        double marketPrice3TradesValue;
        double admittedValue;
        String matDate;
        double duration;
        double yieldAtMap;
        double iriCpiClose;
        double beiClose;
        double couponPercent;
        double couponValue;
        String buyBackDate;
        String lastTradeDate;
        double faceValue;
        String currencyId;
        double cbrClose;
        double yieldToOffer;
        double yieldLastCoupon;
        String offerDate;
        String faceUnit;
        int    tradingSession;

        public StageSecurityRateSharesData(String boardId,
                                           String tradeDate,
                                           String shortName,
                                           String securityId,
                                           double numTrades,
                                           double value,
                                           double low,
                                           double high,
                                           double close,
                                           double legalClosePrice,
                                           double accInt,
                                           double waPrice,
                                           double yieldClose,
                                           double open,
                                           double volume,
                                           double marketPrice2,
                                           double marketPrice3,
                                           double admittedQuote,
                                           double mp2ValTrd,
                                           double marketPrice3TradesValue,
                                           double admittedValue,
                                           String matDate,
                                           double duration,
                                           double yieldAtMap,
                                           double iriCpiClose,
                                           double beiClose,
                                           double couponPercent,
                                           double couponValue,
                                           String buyBackDate,
                                           String lastTradeDate,
                                           double faceValue,
                                           String currencyId,
                                           double cbrClose,
                                           double yieldToOffer,
                                           double yieldLastCoupon,
                                           String offerDate,
                                           String faceUnit,
                                           int    tradingSession) {
            this.boardId = boardId;
            this.tradeDate = tradeDate;
            this.shortName = shortName;
            this.securityId = securityId;
            this.numTrades = numTrades;
            this.value = value;
            this.low = low;
            this.high = high;
            this.close = close;
            this.legalClosePrice = legalClosePrice;
            this.accInt = accInt;
            this.waPrice = waPrice;
            this.yieldClose = yieldClose;
            this.open = open;
            this.volume = volume;
            this.marketPrice2 = marketPrice2;
            this.marketPrice3 = marketPrice3;
            this.admittedQuote = admittedQuote;
            this.mp2ValTrd = mp2ValTrd;
            this.marketPrice3TradesValue = marketPrice3TradesValue;
            this.admittedValue = admittedValue;
            this.matDate = matDate;
            this.duration = duration;
            this.yieldAtMap = yieldAtMap;
            this.iriCpiClose = iriCpiClose;
            this.beiClose = beiClose;
            this.couponPercent = couponPercent;
            this.couponValue = couponValue;
            this.buyBackDate = buyBackDate;
            this.lastTradeDate = lastTradeDate;
            this.faceValue = faceValue;
            this.currencyId = currencyId;
            this.cbrClose = cbrClose;
            this.yieldToOffer = yieldToOffer;
            this.yieldLastCoupon = yieldLastCoupon;
            this.offerDate = offerDate;
            this.faceUnit = faceUnit;
            this.tradingSession = tradingSession;
        }

    }

    public StageSecurityRateBonds(Flow flow) {
        super(flow, "security_rate_bonds");
    }

    @Override
    public void detailLoad(PreparedStatement stmtUpdate) {
        final String objectJSON = "history";
        final String urlStringRaw = "http://iss.moex.com/iss/history/engines/stock/markets/bonds/securities.json" +
                                    "?iss.meta=off&history.columns=";
        final String urlColumnList = "BOARDID,TRADEDATE,SHORTNAME,SECID,NUMTRADES,VALUE,LOW,HIGH,CLOSE,LEGALCLOSEPRICE," +
                                     "ACCINT,WAPRICE,YIELDCLOSE,OPEN,VOLUME,MARKETPRICE2,MARKETPRICE3,ADMITTEDQUOTE," +
                                     "MP2VALTRD,MARKETPRICE3TRADESVALUE,ADMITTEDVALUE,MATDATE,DURATION,YIELDATWAP," +
                                     "IRICPICLOSE,BEICLOSE,COUPONPERCENT,COUPONVALUE,BUYBACKDATE,LASTTRADEDATE,FACEVALUE," +
                                     "CURRENCYID,CBRCLOSE,YIELDTOOFFER,YIELDLASTCOUPON,OFFERDATE,FACEUNIT,TRADINGSESSION";

        final String urlStringData = urlStringRaw.concat(urlColumnList);
        periodMultiLoad(stmtUpdate, urlStringData, this, objectJSON);
    }

    @Override
    public StageSecurityRateSharesData accumulateData(JSONArray jsonArray) {
        StageSecurityRateSharesData data;
        data = new StageSecurityRateSharesData(jsonArray.optString(0), // BOARDID
                                               jsonArray.optString(1), // TRADEDATE
                                               jsonArray.optString(2), // SHORTNAME
                                               jsonArray.optString(3), // SECID
                                               jsonArray.optDouble(4), // NUMTRADES
                                               jsonArray.optDouble(5), // VALUE
                                               jsonArray.optDouble(6), // LOW
                                               jsonArray.optDouble(7), // HIGH
                                               jsonArray.optDouble(8), // CLOSE
                                               jsonArray.optDouble(9), // LEGALCLOSEPRICE
                                               jsonArray.optDouble(10), // ACCINT
                                               jsonArray.optDouble(11), // WAPRICE
                                               jsonArray.optDouble(12), // YIELDCLOSE
                                               jsonArray.optDouble(13), // OPEN
                                               jsonArray.optDouble(14), // VOLUME
                                               jsonArray.optDouble(15), // MARKETPRICE2
                                               jsonArray.optDouble(16), // MARKETPRICE3
                                               jsonArray.optDouble(17), // ADMITTEDQUOTE
                                               jsonArray.optDouble(18), // MP2VALTRD
                                               jsonArray.optDouble(19), // MARKETPRICE3TRADESVALUE
                                               jsonArray.optDouble(20), // ADMITTEDVALUE
                                               jsonArray.optString(21), // MATDATE
                                               jsonArray.optDouble(22), // DURATION
                                               jsonArray.optDouble(23), // YIELDATWAP
                                               jsonArray.optDouble(24), // IRICPICLOSE
                                               jsonArray.optDouble(25), // BEICLOSE
                                               jsonArray.optDouble(26), // COUPONPERCENT
                                               jsonArray.optDouble(27), // COUPONVALUE
                                               jsonArray.optString(28), // BUYBACKDATE
                                               jsonArray.optString(29), // LASTTRADEDATE
                                               jsonArray.optDouble(30), // FACEVALUE
                                               jsonArray.optString(31), // CURRENCYID
                                               jsonArray.optDouble(32), // CBRCLOSE
                                               jsonArray.optDouble(33), // YIELDTOOFFER
                                               jsonArray.optDouble(34), // YIELDLASTCOUPON
                                               jsonArray.optString(35), // OFFERDATE
                                               jsonArray.optString(36), // FACEUNIT
                                               jsonArray.optInt(37)); // TRADINGSESSION
        return data;
    }

    @Override
    public void saveData(PreparedStatement stmtUpdate, List<? extends ExternalData> dataArray) {
        try {
            for(ExternalData iter : dataArray) {
                StageSecurityRateSharesData jter = (StageSecurityRateSharesData) iter;
                stmtUpdate.setLong(1, this.getFlowLoadId());
                stmtUpdate.setString(2, this.getFlowLogStartTimestamp().format(dateTimeFormat));
                stmtUpdate.setString(3, jter.boardId); // BOARDID
                stmtUpdate.setString(4, jter.tradeDate);  // TRADEDATE
                stmtUpdate.setString(5, jter.shortName); // SHORTNAME
                stmtUpdate.setString(6, jter.securityId); // SECID
                stmtUpdate.setDouble(7, jter.numTrades); // NUMTRADES
                stmtUpdate.setDouble(8, jter.value); // VALUE
                stmtUpdate.setDouble(9, jter.low); // LOW
                stmtUpdate.setDouble(10, jter.high); // HIGH
                stmtUpdate.setDouble(11, jter.close); // CLOSE
                stmtUpdate.setDouble(12, jter.legalClosePrice); // LEGALCLOSEPRICE
                stmtUpdate.setDouble(13, jter.accInt); // ACCINT
                stmtUpdate.setDouble(14, jter.waPrice); // WAPRICE
                stmtUpdate.setDouble(15, jter.yieldClose); // YIELDCLOSE
                stmtUpdate.setDouble(16, jter.open); // OPEN
                stmtUpdate.setDouble(17, jter.volume); // VOLUME
                stmtUpdate.setDouble(18, jter.marketPrice2); // MARKETPRICE2
                stmtUpdate.setDouble(19, jter.marketPrice3); // MARKETPRICE3
                stmtUpdate.setDouble(20, jter.admittedQuote); // ADMITTEDQUOTE
                stmtUpdate.setDouble(21, jter.mp2ValTrd); // MP2VALTRD
                stmtUpdate.setDouble(22, jter.marketPrice3TradesValue); // MARKETPRICE3TRADESVALUE
                stmtUpdate.setDouble(23, jter.admittedValue); // ADMITTEDVALUE
                stmtUpdate.setString(24, jter.matDate); // MATDATE
                stmtUpdate.setDouble(25, jter.duration); // DURATION
                stmtUpdate.setDouble(26, jter.yieldAtMap); // YIELDATWAP
                stmtUpdate.setDouble(27, jter.iriCpiClose); // IRICPICLOSE
                stmtUpdate.setDouble(28, jter.beiClose); // BEICLOSE
                stmtUpdate.setDouble(29, jter.couponPercent); // COUPONPERCENT
                stmtUpdate.setDouble(30, jter.couponValue); // COUPONVALUE
                stmtUpdate.setString(31, jter.buyBackDate); // BUYBACKDATE
                stmtUpdate.setString(32, jter.lastTradeDate); // LASTTRADEDATE
                stmtUpdate.setDouble(33, jter.faceValue); // FACEVALUE
                stmtUpdate.setString(34, jter.currencyId); // CURRENCYID
                stmtUpdate.setDouble(35, jter.cbrClose); // CBRCLOSE
                stmtUpdate.setDouble(36, jter.yieldToOffer); // YIELDTOOFFER
                stmtUpdate.setDouble(37, jter.yieldLastCoupon); // YIELDLASTCOUPON
                stmtUpdate.setString(38, jter.offerDate); // OFFERDATE
                stmtUpdate.setString(39, jter.faceUnit); // FACEUNIT
                stmtUpdate.setInt(40, jter.tradingSession); // TRADINGSESSION
                stmtUpdate.addBatch();
            }
            setInsertCount(getInsertCount() + stmtUpdate.executeBatch().length);
            stmtUpdate.clearBatch();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
