package org.data;

import org.flow.Flow;
import org.json.JSONArray;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

import static org.util.AuxUtil.dateTimeFormat;

public class StageSecurityDailyMarketdataBonds extends SnapshotEntity implements ImoexSourceEntity {

    static class StageSecurityDailyMarketdataBondsData extends ExternalData {
        String securityId;
        double bid;
        String bidDepth;
        double offer;
        String offerDepth;
        double spread;
        int    bidDeptht;
        int    offerDeptht;
        double open;
        double low;
        double high;
        double last;
        double lastChange;
        double lastChangePrcnt;
        int    qty;
        double value;
        double yield;
        double valueUsd;
        double waPrice;
        double lastCngToLastWaPrice;
        double wapToPrevWaPricePrcnt;
        double wapToPrevWaPrice;
        double yieldAtWaPrice;
        double yieldToPrevYield;
        double closeYield;
        double closePrice;
        double marketPriceToDay;
        double marketPrice;
        double lastToPevPrice;
        int    numTrades;
        long   volToDay;
        long   valToDay;
        long   valToDayUSD;
        String boardId;
        String tradingStatus;
        String updateTime;
        double duration;
        String numBids;
        String numOffers;
        double change;
        String time;
        String highBid;
        String lowOffer;
        double priceMinusPrevWaPrice;
        String lastBid;
        String lastOffer;
        double lCurrentPrice;
        double lClosePrice;
        double marketPrice2;
        double admittedQuote;
        double openPeriodPrice;
        long   seqNum;
        String sysTime;
        long   valToDayRUR;
        double iriCpiClose;
        double beiClose;
        double cbrClose;
        double yieldToOffer;
        double yieldLastCoupon;
        String tradingSession;


        public StageSecurityDailyMarketdataBondsData(String securityId,
                                                     double bid,
                                                     String bidDepth,
                                                     double offer,
                                                     String offerDepth,
                                                     double spread,
                                                     int    bidDeptht,
                                                     int    offerDeptht,
                                                     double open,
                                                     double low,
                                                     double high,
                                                     double last,
                                                     double lastChange,
                                                     double lastChangePrcnt,
                                                     int    qty,
                                                     double value,
                                                     double yield,
                                                     double valueUsd,
                                                     double waPrice,
                                                     double lastCngToLastWaPrice,
                                                     double wapToPrevWaPricePrcnt,
                                                     double wapToPrevWaPrice,
                                                     double yieldAtWaPrice,
                                                     double yieldToPrevYield,
                                                     double closeYield,
                                                     double closePrice,
                                                     double marketPriceToDay,
                                                     double marketPrice,
                                                     double lastToPevPrice,
                                                     int    numTrades,
                                                     long   volToDay,
                                                     long   valToDay,
                                                     long   valToDayUSD,
                                                     String boardId,
                                                     String tradingStatus,
                                                     String updateTime,
                                                     double duration,
                                                     String numBids,
                                                     String numOffers,
                                                     double change,
                                                     String time,
                                                     String highBid,
                                                     String lowOffer,
                                                     double priceMinusPrevWaPrice,
                                                     String lastBid,
                                                     String lastOffer,
                                                     double lCurrentPrice,
                                                     double lClosePrice,
                                                     double marketPrice2,
                                                     double admittedQuote,
                                                     double openPeriodPrice,
                                                     long   seqNum,
                                                     String sysTime,
                                                     long   valToDayRUR,
                                                     double iriCpiClose,
                                                     double beiClose,
                                                     double cbrClose,
                                                     double yieldToOffer,
                                                     double yieldLastCoupon,
                                                     String tradingSession) {
            this.securityId = securityId;
            this.bid = bid;
            this.bidDepth = bidDepth;
            this.offer = offer;
            this.offerDepth = offerDepth;
            this.spread = spread;
            this.bidDeptht = bidDeptht;
            this.offerDeptht = offerDeptht;
            this.open = open;
            this.low = low;
            this.high = high;
            this.last = last;
            this.lastChange = lastChange;
            this.lastChangePrcnt = lastChangePrcnt;
            this.qty = qty;
            this.value = value;
            this.yield = yield;
            this.valueUsd = valueUsd;
            this.waPrice = waPrice;
            this.lastCngToLastWaPrice = lastCngToLastWaPrice;
            this.wapToPrevWaPricePrcnt = wapToPrevWaPricePrcnt;
            this.wapToPrevWaPrice = wapToPrevWaPrice;
            this.yieldAtWaPrice = yieldAtWaPrice;
            this.yieldToPrevYield = yieldToPrevYield;
            this.closeYield = closeYield;
            this.closePrice = closePrice;
            this.marketPriceToDay = marketPriceToDay;
            this.marketPrice = marketPrice;
            this.lastToPevPrice = lastToPevPrice;
            this.numTrades = numTrades;
            this.volToDay = volToDay;
            this.valToDay = valToDay;
            this.valToDayUSD = valToDayUSD;
            this.boardId = boardId;
            this.tradingStatus = tradingStatus;
            this.updateTime = updateTime;
            this.duration = duration;
            this.numBids = numBids;
            this.numOffers = numOffers;
            this.change = change;
            this.time = time;
            this.highBid = highBid;
            this.lowOffer = lowOffer;
            this.priceMinusPrevWaPrice = priceMinusPrevWaPrice;
            this.lastBid = lastBid;
            this.lastOffer = lastOffer;
            this.lCurrentPrice = lCurrentPrice;
            this.lClosePrice = lClosePrice;
            this.marketPrice2 = marketPrice2;
            this.admittedQuote = admittedQuote;
            this.openPeriodPrice = openPeriodPrice;
            this.seqNum = seqNum;
            this.sysTime = sysTime;
            this.valToDayRUR = valToDayRUR;
            this.iriCpiClose = iriCpiClose;
            this.beiClose = beiClose;
            this.cbrClose = cbrClose;
            this.yieldToOffer = yieldToOffer;
            this.yieldLastCoupon = yieldLastCoupon;
            this.tradingSession = tradingSession;
        }

    }

    public StageSecurityDailyMarketdataBonds(Flow flow) {
        super(flow, "security_daily_marketdata_bonds");
    }

    @Override
    public void detailLoad(PreparedStatement stmtUpdate) {
        final String objectJSON = "marketdata";
        String urlStringRaw = "http://iss.moex.com/iss/engines/stock/markets/bonds/securities.json" +
                              "?iss.meta=off&iss.only=marketdata&marketdata.columns=";
        String urlColumnList = "SECID,BID,BIDDEPTH,OFFER,OFFERDEPTH,SPREAD,BIDDEPTHT,OFFERDEPTHT,OPEN,LOW,HIGH,LAST," +
                               "LASTCHANGE,LASTCHANGEPRCNT,QTY,VALUE,YIELD,VALUE_USD,WAPRICE,LASTCNGTOLASTWAPRICE," +
                               "WAPTOPREVWAPRICEPRCNT,WAPTOPREVWAPRICE,YIELDATWAPRICE,YIELDTOPREVYIELD,CLOSEYIELD," +
                               "CLOSEPRICE,MARKETPRICETODAY,MARKETPRICE,LASTTOPREVPRICE,NUMTRADES,VOLTODAY,VALTODAY," +
                               "VALTODAY_USD,BOARDID,TRADINGSTATUS,UPDATETIME,DURATION,NUMBIDS,NUMOFFERS,CHANGE,TIME," +
                               "HIGHBID,LOWOFFER,PRICEMINUSPREVWAPRICE,LASTBID,LASTOFFER,LCURRENTPRICE,LCLOSEPRICE," +
                               "MARKETPRICE2,ADMITTEDQUOTE,OPENPERIODPRICE,SEQNUM,SYSTIME,VALTODAY_RUR,IRICPICLOSE," +
                               "BEICLOSE,CBRCLOSE,YIELDTOOFFER,YIELDLASTCOUPON,TRADINGSESSION";

        String urlStringData = urlStringRaw.concat(urlColumnList);
        saveData(stmtUpdate, load(urlStringData, objectJSON));
    }

    @Override
    public StageSecurityDailyMarketdataBondsData accumulateData(JSONArray jsonArray) {
        StageSecurityDailyMarketdataBondsData data;
        data = new StageSecurityDailyMarketdataBondsData(jsonArray.optString(0), // SECID
                                                         jsonArray.optDouble(1), // BID
                                                         jsonArray.optString(2), // BIDDEPTH
                                                         jsonArray.optDouble(3), // OFFER
                                                         jsonArray.optString(4), // OFFERDEPTH
                                                         jsonArray.optDouble(5), // SPREAD
                                                         jsonArray.optInt(6), // BIDDEPTHT
                                                         jsonArray.optInt(7), // OFFERDEPTHT
                                                         jsonArray.optDouble(8), // OPEN
                                                         jsonArray.optDouble(9), // LOW
                                                         jsonArray.optDouble(10), // HIGH
                                                         jsonArray.optDouble(11), // LAST
                                                         jsonArray.optDouble(12), // LASTCHANGE
                                                         jsonArray.optDouble(13), // LASTCHANGEPRCNT
                                                         jsonArray.optInt(14), // QTY
                                                         jsonArray.optDouble(15), // VALUE
                                                         jsonArray.optDouble(16), // YIELD
                                                         jsonArray.optDouble(17), // VALUE_USD
                                                         jsonArray.optDouble(18), // WAPRICE
                                                         jsonArray.optDouble(19), // LASTCNGTOLASTWAPRICE
                                                         jsonArray.optDouble(20), // WAPTOPREVWAPRICEPRCNT
                                                         jsonArray.optDouble(21), // WAPTOPREVWAPRICE
                                                         jsonArray.optDouble(22), // YIELDATWAPRICE
                                                         jsonArray.optDouble(23), // YIELDTOPREVYIELD
                                                         jsonArray.optDouble(24), // CLOSEYIELD
                                                         jsonArray.optDouble(25), // CLOSEPRICE
                                                         jsonArray.optDouble(26), // MARKETPRICETODAY
                                                         jsonArray.optDouble(27), // MARKETPRICE
                                                         jsonArray.optDouble(28), // LASTTOPREVPRICE
                                                         jsonArray.optInt(29), // NUMTRADES
                                                         jsonArray.optLong(30), // VOLTODAY
                                                         jsonArray.optLong(31), // VALTODAY
                                                         jsonArray.optLong(32), // VALTODAY_USD
                                                         jsonArray.optString(33), // BOARDID
                                                         jsonArray.optString(34), // TRADINGSTATUS
                                                         jsonArray.optString(35), // UPDATETIME
                                                         jsonArray.optDouble(36), // DURATION
                                                         jsonArray.optString(37), // NUMBIDS
                                                         jsonArray.optString(38), // NUMOFFERS
                                                         jsonArray.optDouble(39), // CHANGE
                                                         jsonArray.optString(40), // TIME
                                                         jsonArray.optString(41), // HIGHBID
                                                         jsonArray.optString(42), // LOWOFFER
                                                         jsonArray.optDouble(43), // PRICEMINUSPREVWAPRICE
                                                         jsonArray.optString(44), // LASTBID
                                                         jsonArray.optString(45), // LASTOFFER
                                                         jsonArray.optDouble(46), // LCURRENTPRICE
                                                         jsonArray.optDouble(47), // LCLOSEPRICE
                                                         jsonArray.optDouble(48), // MARKETPRICE2
                                                         jsonArray.optDouble(49), // ADMITTEDQUOTE
                                                         jsonArray.optDouble(50), // OPENPERIODPRICE
                                                         jsonArray.optLong(51), // SEQNUM
                                                         jsonArray.optString(52), // SYSTIME
                                                         jsonArray.optLong(53), // VALTODAY_RUR
                                                         jsonArray.optDouble(54), // IRICPICLOSE
                                                         jsonArray.optDouble(55), // BEICLOSE
                                                         jsonArray.optDouble(56), // CBRCLOSE
                                                         jsonArray.optDouble(57), // YIELDTOOFFER
                                                         jsonArray.optDouble(58), // YIELDLASTCOUPON
                                                         jsonArray.optString(59)); // TRADINGSESSION
        return data;
    }

    @Override
    public void saveData(PreparedStatement stmtUpdate, List<? extends ExternalData> dataArray) {
        try {
            for(ExternalData iter : dataArray) {
                StageSecurityDailyMarketdataBondsData jter = (StageSecurityDailyMarketdataBondsData) iter;
                stmtUpdate.setLong(1, this.getFlowLoadId());
                stmtUpdate.setString(2, this.getFlowLogStartTimestamp().format(dateTimeFormat));
                stmtUpdate.setString(3, jter.securityId); // SECID
                stmtUpdate.setDouble(4, jter.bid); // BID
                stmtUpdate.setString(5, jter.bidDepth); // BIDDEPTH
                stmtUpdate.setDouble(6, jter.offer); // OFFER
                stmtUpdate.setString(7, jter.offerDepth); // OFFERDEPTH
                stmtUpdate.setDouble(8, jter.spread); // SPREAD
                stmtUpdate.setInt(9, jter.bidDeptht); // BIDDEPTHT
                stmtUpdate.setInt(10, jter.offerDeptht); // OFFERDEPTHT
                stmtUpdate.setDouble(11, jter.open); // OPEN
                stmtUpdate.setDouble(12, jter.low); // LOW
                stmtUpdate.setDouble(13, jter.high); // HIGH
                stmtUpdate.setDouble(14, jter.last); // LAST
                stmtUpdate.setDouble(15, jter.lastChange); // LASTCHANGE
                stmtUpdate.setDouble(16, jter.lastChangePrcnt); // LASTCHANGEPRCNT
                stmtUpdate.setInt(17, jter.qty); // QTY
                stmtUpdate.setDouble(18, jter.value); // VALUE
                stmtUpdate.setDouble(19, jter.yield); // YIELD
                stmtUpdate.setDouble(20, jter.valueUsd); // VALUE_USD
                stmtUpdate.setDouble(21, jter.waPrice); // WAPRICE
                stmtUpdate.setDouble(22, jter.lastCngToLastWaPrice); // LASTCNGTOLASTWAPRICE
                stmtUpdate.setDouble(23, jter.wapToPrevWaPricePrcnt); // WAPTOPREVWAPRICEPRCNT
                stmtUpdate.setDouble(24, jter.wapToPrevWaPrice); // WAPTOPREVWAPRICE
                stmtUpdate.setDouble(25, jter.yieldAtWaPrice); // YIELDATWAPRICE
                stmtUpdate.setDouble(26, jter.yieldToPrevYield); // YIELDTOPREVYIELD
                stmtUpdate.setDouble(27, jter.closeYield); // CLOSEYIELD
                stmtUpdate.setDouble(28, jter.closePrice); // CLOSEPRICE
                stmtUpdate.setDouble(29, jter.marketPriceToDay); // MARKETPRICETODAY
                stmtUpdate.setDouble(30, jter.marketPrice); // MARKETPRICE
                stmtUpdate.setDouble(31, jter.lastToPevPrice); // LASTTOPREVPRICE
                stmtUpdate.setInt(32, jter.numTrades); // NUMTRADES
                stmtUpdate.setLong(33, jter.volToDay); // VOLTODAY
                stmtUpdate.setLong(34, jter.valToDay); // VALTODAY
                stmtUpdate.setLong(35, jter.valToDayUSD); // VALTODAY_USD
                stmtUpdate.setString(36, jter.boardId); // BOARDID
                stmtUpdate.setString(37, jter.tradingStatus); // TRADINGSTATUS
                stmtUpdate.setString(38, jter.updateTime); // UPDATETIME
                stmtUpdate.setDouble(39, jter.duration); // DURATION
                stmtUpdate.setString(40, jter.numBids); // NUMBIDS
                stmtUpdate.setString(41, jter.numOffers); // NUMOFFERS
                stmtUpdate.setDouble(42, jter.change); // CHANGE
                stmtUpdate.setString(43, jter.time); // TIME
                stmtUpdate.setString(44, jter.highBid); // HIGHBID
                stmtUpdate.setString(45, jter.lowOffer); // LOWOFFER
                stmtUpdate.setDouble(46, jter.priceMinusPrevWaPrice); // PRICEMINUSPREVWAPRICE
                stmtUpdate.setString(47, jter.lastBid); // LASTBID
                stmtUpdate.setString(48, jter.lastOffer); // LASTOFFER
                stmtUpdate.setDouble(49, jter.lCurrentPrice); // LCURRENTPRICE
                stmtUpdate.setDouble(50, jter.lClosePrice); // LCLOSEPRICE
                stmtUpdate.setDouble(51, jter.marketPrice2); // MARKETPRICE2
                stmtUpdate.setDouble(52, jter.admittedQuote); // ADMITTEDQUOTE
                stmtUpdate.setDouble(53, jter.openPeriodPrice); // OPENPERIODPRICE
                stmtUpdate.setLong(54, jter.seqNum); // SEQNUM
                stmtUpdate.setString(55, jter.sysTime); // SYSTIME
                stmtUpdate.setLong(56, jter.valToDayRUR); // VALTODAY_RUR
                stmtUpdate.setDouble(57, jter.iriCpiClose); // IRICPICLOSE
                stmtUpdate.setDouble(58, jter.beiClose); // BEICLOSE
                stmtUpdate.setDouble(59, jter.cbrClose); // CBRCLOSE
                stmtUpdate.setDouble(60, jter.yieldToOffer); // YIELDTOOFFER
                stmtUpdate.setDouble(61, jter.yieldLastCoupon); // YIELDLASTCOUPON
                stmtUpdate.setString(62, jter.tradingSession); // TRADINGSESSION
                stmtUpdate.addBatch();
            }
            setInsertCount(getInsertCount() + stmtUpdate.executeBatch().length);
            stmtUpdate.clearBatch();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
