package org.data;

import org.flow.Flow;
import org.json.JSONArray;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

import static org.util.AuxUtil.dateTimeFormat;

public class StageSecurityDailyMarketdataShares extends SnapshotEntity implements ImoexSourceEntity {

    static class StageSecurityDailyMarketdataSharesData extends ExternalData {
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


        public StageSecurityDailyMarketdataSharesData(String securityId,
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

    public StageSecurityDailyMarketdataShares(Flow flow) {
        super(flow, "security_daily_marketdata_shares");
    }

    @Override
    public void detailLoad(PreparedStatement stmtUpdate) {
        final String objectJSON = "marketdata";
        String urlStringRaw = "http://iss.moex.com/iss/engines/stock/markets/shares/securities.json" +
                              "?iss.meta=off&iss.only=marketdata&marketdata.columns=";
        String urlColumnList = "SECID,BOARDID,BID,BIDDEPTH,OFFER,OFFERDEPTH,SPREAD,BIDDEPTHT,OFFERDEPTHT,OPEN,LOW," +
                               "HIGH,LAST,LASTCHANGE,LASTCHANGEPRCNT,QTY,VALUE,VALUE_USD,WAPRICE,LASTCNGTOLASTWAPRICE," +
                               "WAPTOPREVWAPRICEPRCNT,WAPTOPREVWAPRICE,CLOSEPRICE,MARKETPRICETODAY,MARKETPRICE," +
                               "LASTTOPREVPRICE,NUMTRADES,VOLTODAY,VALTODAY,VALTODAY_USD,ETFSETTLEPRICE,TRADINGSTATUS," +
                               "UPDATETIME,ADMITTEDQUOTE,LASTBID,LASTOFFER,LCLOSEPRICE,LCURRENTPRICE,MARKETPRICE2," +
                               "NUMBIDS,NUMOFFERS,CHANGE,TIME,HIGHBID,LOWOFFER,PRICEMINUSPREVWAPRICE,OPENPERIODPRICE," +
                               "SEQNUM,SYSTIME,CLOSINGAUCTIONPRICE,CLOSINGAUCTIONVOLUME,ISSUECAPITALIZATION," +
                               "ISSUECAPITALIZATION_UPDATETIME,ETFSETTLECURRENCY,VALTODAY_RUR,TRADINGSESSION";

        String urlStringData = urlStringRaw.concat(urlColumnList);
        saveData(stmtUpdate, load(urlStringData, objectJSON));

    }

    @Override
    public StageSecurityDailyMarketdataSharesData accumulateData(JSONArray jsonArray) {
        StageSecurityDailyMarketdataSharesData data;
        data = new StageSecurityDailyMarketdataSharesData(jsonArray.optString(0), // SECID
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
                                                          jsonArray.optString(55)); // TRADINGSESSION
        return data;
    }

    @Override
    public void saveData(PreparedStatement stmtUpdate, List<? extends ExternalData> dataArray) {
        try {
            for(ExternalData iter : dataArray) {
                StageSecurityDailyMarketdataSharesData jter = (StageSecurityDailyMarketdataSharesData) iter;
                stmtUpdate.setLong(1, this.getFlowLoadId());
                stmtUpdate.setString(2, this.getFlowLogStartTimestamp().format(dateTimeFormat));
                stmtUpdate.setString(3, jter.securityId);
                stmtUpdate.setString(4, jter.boardId);
                stmtUpdate.setDouble(5, jter.bid);
                stmtUpdate.setString(6, jter.bidDepth);
                stmtUpdate.setDouble(7, jter.offer);
                stmtUpdate.setString(8, jter.offerDepth);
                stmtUpdate.setDouble(9, jter.spread);
                stmtUpdate.setInt(10, jter.bidDeptht);
                stmtUpdate.setInt(11, jter.offerDeptht);
                stmtUpdate.setDouble(12, jter.open);
                stmtUpdate.setDouble(13, jter.low);
                stmtUpdate.setDouble(14, jter.high);
                stmtUpdate.setDouble(15, jter.last);
                stmtUpdate.setDouble(16, jter.lastChange);
                stmtUpdate.setDouble(17, jter.lastChangePrcnt);
                stmtUpdate.setInt(18, jter.qty);
                stmtUpdate.setDouble(19, jter.value);
                stmtUpdate.setDouble(20, jter.value_usd);
                stmtUpdate.setDouble(21, jter.waPrice);
                stmtUpdate.setDouble(22, jter.lastCngToLastWaPrice);
                stmtUpdate.setDouble(23, jter.wapToPrevWaPricePrcnt);
                stmtUpdate.setDouble(24, jter.wapToPrevWaPrice);
                stmtUpdate.setDouble(25, jter.closePrice);
                stmtUpdate.setDouble(26, jter.marketPriceToDay);
                stmtUpdate.setDouble(27, jter.marketPrice);
                stmtUpdate.setDouble(28, jter.lastToPevPrice);
                stmtUpdate.setInt(29, jter.numTrades);
                stmtUpdate.setLong(30, jter.volToDay);
                stmtUpdate.setLong(31, jter.valToDay);
                stmtUpdate.setLong(32, jter.valToDayUSD);
                stmtUpdate.setDouble(33, jter.etfSettlePrice);
                stmtUpdate.setString(34, jter.tradingStatus);
                stmtUpdate.setString(35, jter.updateTime);
                stmtUpdate.setDouble(36, jter.admittedQuote);
                stmtUpdate.setString(37, jter.lastBid);
                stmtUpdate.setString(38, jter.lastOffer);
                stmtUpdate.setDouble(39, jter.lClosePrice);
                stmtUpdate.setDouble(40, jter.lCurrentPrice);
                stmtUpdate.setDouble(41, jter.marketPrice2);
                stmtUpdate.setString(42, jter.numBids);
                stmtUpdate.setString(43, jter.numOffers);
                stmtUpdate.setDouble(44, jter.change);
                stmtUpdate.setString(45, jter.time);
                stmtUpdate.setString(46, jter.highBid);
                stmtUpdate.setString(47, jter.lowOffer);
                stmtUpdate.setDouble(48, jter.priceMinusPrevWaPrice);
                stmtUpdate.setDouble(49, jter.openPeriodPrice);
                stmtUpdate.setLong(50, jter.seqNum);
                stmtUpdate.setString(51, jter.sysTime);
                stmtUpdate.setDouble(52, jter.closingAuctionPrice);
                stmtUpdate.setDouble(53, jter.closingAuctionVolume);
                stmtUpdate.setDouble(54, jter.issueCapitalizationVolume);
                stmtUpdate.setString(55, jter.getIssueCapitalizationUpdateTime);
                stmtUpdate.setString(56, jter.etfSettleCurrency);
                stmtUpdate.setLong(57, jter.valToDayRUR);
                stmtUpdate.setString(58, jter.tradingSession);
                stmtUpdate.setLong(57, this.getFlowLoadId());
                stmtUpdate.addBatch();
            }
            setInsertCount(getInsertCount() + stmtUpdate.executeBatch().length);
            stmtUpdate.clearBatch();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
