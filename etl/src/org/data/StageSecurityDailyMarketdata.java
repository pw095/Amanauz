package org.data;

import org.json.JSONArray;

import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

public class StageSecurityDailyMarketdata extends ImoexWebSiteEntity {

    static class StageSecurityDailyMarketdataData extends ImoexData {
        String securityId;
        String boardId;
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
        double value_usd;
        double waPrice;
        double lastCngToLastWaPrice;
        double wapToPrevWaPricePrcnt;
        double wapToPrevWaPrice;
        double closePrice;
        double marketPriceToDay;
        double marketPrice;
        double lastToPevPrice;
        int    numTrades;
        long   volToDay;
        long   valToDay;
        long   valToDayUSD;
        double etfSettlePrice;
        String tradingStatus;
        String updateTime;
        double admittedQuote;
        String lastBid;
        String lastOffer;
        double lClosePrice;
        double lCurrentPrice;
        double marketPrice2;
        String numBids;
        String numOffers;
        double change;
        String time;
        String highBid;
        String lowOffer;
        double priceMinusPrevWaPrice;
        double openPeriodPrice;
        long   seqNum;
        String sysTime;
        double closingAuctionPrice;
        double closingAuctionVolume;
        double issueCapitalizationVolume;
        String getIssueCapitalizationUpdateTime;
        String etfSettleCurrency;
        long   valToDayRUR;
        String tradingSession;


        public StageSecurityDailyMarketdataData(String securityId,
                                                String boardId,
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
                                                double value_usd,
                                                double waPrice,
                                                double lastCngToLastWaPrice,
                                                double wapToPrevWaPricePrcnt,
                                                double wapToPrevWaPrice,
                                                double closePrice,
                                                double marketPriceToDay,
                                                double marketPrice,
                                                double lastToPevPrice,
                                                int    numTrades,
                                                long   volToDay,
                                                long   valToDay,
                                                long   valToDayUSD,
                                                double etfSettlePrice,
                                                String tradingStatus,
                                                String updateTime,
                                                double admittedQuote,
                                                String lastBid,
                                                String lastOffer,
                                                double lClosePrice,
                                                double lCurrentPrice,
                                                double marketPrice2,
                                                String numBids,
                                                String numOffers,
                                                double change,
                                                String time,
                                                String highBid,
                                                String lowOffer,
                                                double priceMinusPrevWaPrice,
                                                double openPeriodPrice,
                                                long   seqNum,
                                                String sysTime,
                                                double closingAuctionPrice,
                                                double closingAuctionVolume,
                                                double issueCapitalizationVolume,
                                                String getIssueCapitalizationUpdateTime,
                                                String etfSettleCurrency,
                                                long   valToDayRUR,
                                                String tradingSession) {

            this.securityId = securityId;
            this.boardId = boardId;
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
            this.value_usd = value_usd;
            this.waPrice = waPrice;
            this.lastCngToLastWaPrice = lastCngToLastWaPrice;
            this.wapToPrevWaPricePrcnt = wapToPrevWaPricePrcnt;
            this.wapToPrevWaPrice = wapToPrevWaPrice;
            this.closePrice = closePrice;
            this.marketPriceToDay = marketPriceToDay;
            this.marketPrice = marketPrice;
            this.lastToPevPrice = lastToPevPrice;
            this.numTrades = numTrades;
            this.volToDay = volToDay;
            this.valToDay = valToDay;
            this.valToDayUSD = valToDayUSD;
            this.etfSettlePrice = etfSettlePrice;
            this.tradingStatus = tradingStatus;
            this.updateTime = updateTime;
            this.admittedQuote = admittedQuote;
            this.lastBid = lastBid;
            this.lastOffer = lastOffer;
            this.lClosePrice = lClosePrice;
            this.lCurrentPrice = lCurrentPrice;
            this.marketPrice2 = marketPrice2;
            this.numBids = numBids;
            this.numOffers = numOffers;
            this.change = change;
            this.time = time;
            this.highBid = highBid;
            this.lowOffer = lowOffer;
            this.priceMinusPrevWaPrice = priceMinusPrevWaPrice;
            this.openPeriodPrice = openPeriodPrice;
            this.seqNum = seqNum;
            this.sysTime = sysTime;
            this.closingAuctionPrice = closingAuctionPrice;
            this.closingAuctionVolume = closingAuctionVolume;
            this.issueCapitalizationVolume = issueCapitalizationVolume;
            this.getIssueCapitalizationUpdateTime = getIssueCapitalizationUpdateTime;
            this.etfSettleCurrency = etfSettleCurrency;
            this.valToDayRUR = valToDayRUR;
            this.tradingSession = tradingSession;
        }

    }

    public StageSecurityDailyMarketdata(long flowLoadId) {
        super(flowLoadId, "security_daily_marketdata"/*, loadMode*/);
    }

    public void detailLoad(PreparedStatement stmtUpdate) {
//        http://iss.moex.com/iss/engines/stock/markets/bonds;
        String urlStringRaw = "http://iss.moex.com/iss/engines/stock/markets/";
        String urlStringData;
        String urlColumnList = "/securities.json?iss.meta=off&iss.only=marketdata&marketdata.columns=SECID,BOARDID,BID,BIDDEPTH,OFFER,OFFERDEPTH,SPREAD,BIDDEPTHT,OFFERDEPTHT,OPEN,LOW,HIGH,LAST,LASTCHANGE,LASTCHANGEPRCNT,QTY,VALUE,VALUE_USD,WAPRICE,LASTCNGTOLASTWAPRICE,WAPTOPREVWAPRICEPRCNT,WAPTOPREVWAPRICE,CLOSEPRICE,MARKETPRICETODAY,MARKETPRICE,LASTTOPREVPRICE,NUMTRADES,VOLTODAY,VALTODAY,VALTODAY_USD,ETFSETTLEPRICE,TRADINGSTATUS,UPDATETIME,ADMITTEDQUOTE,LASTBID,LASTOFFER,LCLOSEPRICE,LCURRENTPRICE,MARKETPRICE2,NUMBIDS,NUMOFFERS,CHANGE,TIME,HIGHBID,LOWOFFER,PRICEMINUSPREVWAPRICE,OPENPERIODPRICE,SEQNUM,SYSTIME,CLOSINGAUCTIONPRICE,CLOSINGAUCTIONVOLUME,ISSUECAPITALIZATION,ISSUECAPITALIZATION_UPDATETIME,ETFSETTLECURRENCY,VALTODAY_RUR,TRADINGSESSION";
        String urlStringMeta = null;
        urlStringData = urlStringRaw.concat("shares").concat(urlColumnList);

        try {
            singleIterationLoad(stmtUpdate, urlStringData, "marketdata");
        } catch (Exception e) {
            e.printStackTrace();
        }

        urlStringData = urlStringRaw.concat("bonds").concat(urlColumnList);

        try {
            singleIterationLoad(stmtUpdate, urlStringData, "marketdata");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    StageSecurityDailyMarketdataData accumulateData(JSONArray jsonArray) {
        StageSecurityDailyMarketdataData data;
        data = new StageSecurityDailyMarketdataData(jsonArray.optString(0), // SECID
                jsonArray.optString(1), // BOARDID
                jsonArray.optDouble(2), // BID
                jsonArray.optString(3), // BIDDEPTH
                jsonArray.optDouble(4), // OFFER
                jsonArray.optString(5), // OFFERDEPTH
                jsonArray.optDouble(6), // SPREAD
                jsonArray.optInt(7), // BIDDEPTHT
                jsonArray.optInt(8), // OFFERDEPTHT
                jsonArray.optDouble(9), // OPEN
                jsonArray.optDouble(10), // LOW
                jsonArray.optDouble(11), // HIGH
                jsonArray.optDouble(12), // LAST
                jsonArray.optDouble(13), // LASTCHANGE
                jsonArray.optDouble(14), // LASTCHANGEPRCNT
                jsonArray.optInt(15), // QTY
                jsonArray.optDouble(16), // VALUE
                jsonArray.optDouble(17), // VALUE_USD
                jsonArray.optDouble(18), // WAPRICE
                jsonArray.optDouble(19), // LASTCNGTOLASTWAPRICE
                jsonArray.optDouble(20), // WAPTOPREVWAPRICEPRCNT
                jsonArray.optDouble(21), // WAPTOPREVWAPRICE
                jsonArray.optDouble(22), // CLOSEPRICE
                jsonArray.optDouble(23), // MARKETPRICETODAY
                jsonArray.optDouble(24), // MARKETPRICE
                jsonArray.optDouble(25), // LASTTOPREVPRICE
                jsonArray.optInt(26), // NUMTRADES
                jsonArray.optLong(27), // VOLTODAY
                jsonArray.optLong(28), // VALTODAY
                jsonArray.optLong(29), // VALTODAY_USD
                jsonArray.optDouble(30), // ETFSETTLEPRICE
                jsonArray.optString(31), // TRADINGSTATUS
                jsonArray.optString(32), // UPDATETIME
                jsonArray.optDouble(33), // ADMITTEDQUOTE
                jsonArray.optString(34), // LASTBID
                jsonArray.optString(35), // LASTOFFER
                jsonArray.optDouble(36), // LCLOSEPRICE
                jsonArray.optDouble(37), // LCURRENTPRICE
                jsonArray.optDouble(38), // MARKETPRICE2
                jsonArray.optString(39), // NUMBIDS
                jsonArray.optString(40), // NUMOFFERS
                jsonArray.optDouble(41), // CHANGE
                jsonArray.optString(42), // TIME
                jsonArray.optString(43), // HIGHBID
                jsonArray.optString(44), // LOWOFFER
                jsonArray.optDouble(45), // PRICEMINUSPREVWAPRICE
                jsonArray.optDouble(46), // OPENPERIODPRICE
                jsonArray.optLong(47), // SEQNUM
                jsonArray.optString(48), // SYSTIME
                jsonArray.optDouble(49), // CLOSINGAUCTIONPRICE
                jsonArray.optDouble(50), // CLOSINGAUCTIONVOLUME
                jsonArray.optDouble(51), // ISSUECAPITALIZATION
                jsonArray.optString(52), // ISSUECAPITALIZATION_UPDATETIME
                jsonArray.optString(53), // ETFSETTLECURRENCY
                jsonArray.optLong(54), // VALTODAY_RUR
                jsonArray.optString(55) // TRADINGSESSION
                );
        return data;
    }

    void saveData(PreparedStatement stmtUpdate, List<? extends ImoexData> dataArray) {
        try {
            System.out.println("iter.count = " + dataArray.size());
            for(ImoexData iter : dataArray) {
                StageSecurityDailyMarketdataData jter = (StageSecurityDailyMarketdataData) iter;
//            System.out.println(jter.tradeDate);

                stmtUpdate.setString(1, jter.securityId);
                stmtUpdate.setString(2, jter.boardId);
                stmtUpdate.setDouble(3, jter.bid);
                stmtUpdate.setString(4, jter.bidDepth);
                stmtUpdate.setDouble(5, jter.offer);
                stmtUpdate.setString(6, jter.offerDepth);
                stmtUpdate.setDouble(7, jter.spread);
                stmtUpdate.setInt(8, jter.bidDeptht);
                stmtUpdate.setInt(9, jter.offerDeptht);
                stmtUpdate.setDouble(10, jter.open);
                stmtUpdate.setDouble(11, jter.low);
                stmtUpdate.setDouble(12, jter.high);
                stmtUpdate.setDouble(13, jter.last);
                stmtUpdate.setDouble(14, jter.lastChange);
                stmtUpdate.setDouble(15, jter.lastChangePrcnt);
                stmtUpdate.setInt(16, jter.qty);
                stmtUpdate.setDouble(17, jter.value);
                stmtUpdate.setDouble(18, jter.value_usd);
                stmtUpdate.setDouble(19, jter.waPrice);
                stmtUpdate.setDouble(20, jter.lastCngToLastWaPrice);
                stmtUpdate.setDouble(21, jter.wapToPrevWaPricePrcnt);
                stmtUpdate.setDouble(22, jter.wapToPrevWaPrice);
                stmtUpdate.setDouble(23, jter.closePrice);
                stmtUpdate.setDouble(24, jter.marketPriceToDay);
                stmtUpdate.setDouble(25, jter.marketPrice);
                stmtUpdate.setDouble(26, jter.lastToPevPrice);
                stmtUpdate.setInt(27, jter.numTrades);
                stmtUpdate.setLong(28, jter.volToDay);
                stmtUpdate.setLong(29, jter.valToDay);
                stmtUpdate.setLong(30, jter.valToDayUSD);
                stmtUpdate.setDouble(31, jter.etfSettlePrice);
                stmtUpdate.setString(32, jter.tradingStatus);
                stmtUpdate.setString(33, jter.updateTime);
                stmtUpdate.setDouble(34, jter.admittedQuote);
                stmtUpdate.setString(35, jter.lastBid);
                stmtUpdate.setString(36, jter.lastOffer);
                stmtUpdate.setDouble(37, jter.lClosePrice);
                stmtUpdate.setDouble(38, jter.lCurrentPrice);
                stmtUpdate.setDouble(39, jter.marketPrice2);
                stmtUpdate.setString(40, jter.numBids);
                stmtUpdate.setString(41, jter.numOffers);
                stmtUpdate.setDouble(42, jter.change);
                stmtUpdate.setString(43, jter.time);
                stmtUpdate.setString(44, jter.highBid);
                stmtUpdate.setString(45, jter.lowOffer);
                stmtUpdate.setDouble(46, jter.priceMinusPrevWaPrice);
                stmtUpdate.setDouble(47, jter.openPeriodPrice);
                stmtUpdate.setLong(48, jter.seqNum);
                stmtUpdate.setString(49, jter.sysTime);
                stmtUpdate.setDouble(50, jter.closingAuctionPrice);
                stmtUpdate.setDouble(51, jter.closingAuctionVolume);
                stmtUpdate.setDouble(52, jter.issueCapitalizationVolume);
                stmtUpdate.setString(53, jter.getIssueCapitalizationUpdateTime);
                stmtUpdate.setString(54, jter.etfSettleCurrency);
                stmtUpdate.setLong(55, jter.valToDayRUR);
                stmtUpdate.setString(56, jter.tradingSession);

                stmtUpdate.setLong(57, this.getFlowLoadId());
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
