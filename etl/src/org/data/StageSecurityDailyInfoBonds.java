package org.data;

import org.flow.Flow;
import org.json.JSONArray;
import org.meta.MetaLayer;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

import static org.util.AuxUtil.dateTimeFormat;

public class StageSecurityDailyInfoBonds extends SnapshotEntity implements ImoexSourceEntity {

    static class StageSecurityDailyInfoBondsData extends ExternalData {
        String securityId;
        String boardId;
        String shortName;
        double previousWaPrice;
        double yieldDatePrevWaPrice;
        double couponValue;
        String nextCoupon;
        double accruedInt;
        double previousPrice;
        int    lotSize;
        double faceValue;
        String boardName;
        String status;
        String matDate;
        int    decimals;
        int    couponPeriod;
        long   issueSize;
        double previousLegalClosePrice;
        double previousAdmittedQuote;
        String previousDate;
        String securityName;
        String remarks;
        String marketCode;
        String instrId;
        String sectorId;
        double minStep;
        String faceUnit;
        double buyBackPrice;
        String buyBackDate;
        String isin;
        String latName;
        String regNumber;
        String currencyId;
        long   issueSizePlaced;
        int    listLevel;
        String securityType;
        double couponPercent;
        String offerDate;
        String settleDate;
        double lotValue;


        public StageSecurityDailyInfoBondsData(String securityId,
                                               String boardId,
                                               String shortName,
                                               double previousWaPrice,
                                               double yieldDatePrevWaPrice,
                                               double couponValue,
                                               String nextCoupon,
                                               double accruedInt,
                                               double previousPrice,
                                               int    lotSize,
                                               double faceValue,
                                               String boardName,
                                               String status,
                                               String matDate,
                                               int    decimals,
                                               int    couponPeriod,
                                               long   issueSize,
                                               double previousLegalClosePrice,
                                               double previousAdmittedQuote,
                                               String previousDate,
                                               String securityName,
                                               String remarks,
                                               String marketCode,
                                               String instrId,
                                               String sectorId,
                                               double minStep,
                                               String faceUnit,
                                               double buyBackPrice,
                                               String buyBackDate,
                                               String isin,
                                               String latName,
                                               String regNumber,
                                               String currencyId,
                                               long   issueSizePlaced,
                                               int    listLevel,
                                               String securityType,
                                               double couponPercent,
                                               String offerDate,
                                               String settleDate,
                                               double lotValue) {
            this.securityId = securityId;
            this.boardId = boardId;
            this.shortName = shortName;
            this.previousWaPrice = previousWaPrice;
            this.yieldDatePrevWaPrice = yieldDatePrevWaPrice;
            this.couponValue = couponValue;
            this.nextCoupon = nextCoupon;
            this.accruedInt = accruedInt;
            this.previousPrice = previousPrice;
            this.lotSize = lotSize;
            this.faceValue = faceValue;
            this.boardName = boardName;
            this.status = status;
            this.matDate = matDate;
            this.decimals = decimals;
            this.couponPeriod = couponPeriod;
            this.issueSize = issueSize;
            this.previousLegalClosePrice = previousLegalClosePrice;
            this.previousAdmittedQuote = previousAdmittedQuote;
            this.previousDate = previousDate;
            this.securityName = securityName;
            this.remarks = remarks;
            this.marketCode = marketCode;
            this.instrId = instrId;
            this.sectorId = sectorId;
            this.minStep = minStep;
            this.faceUnit = faceUnit;
            this.buyBackPrice = buyBackPrice;
            this.buyBackDate = buyBackDate;
            this.isin = isin;
            this.latName = latName;
            this.regNumber = regNumber;
            this.currencyId = currencyId;
            this.issueSizePlaced = issueSizePlaced;
            this.listLevel = listLevel;
            this.securityType = securityType;
            this.couponPercent = couponPercent;
            this.offerDate = offerDate;
            this.settleDate = settleDate;
            this.lotValue = lotValue;
        }
    }

    public StageSecurityDailyInfoBonds(Flow flow) {
        super(flow, MetaLayer.STAGE, "security_daily_info_bonds");
    }

    @Override
    public void callLoad(Connection conn) {
        concreteLoad(conn);
    }

    @Override
    public void detailLoad(PreparedStatement stmtUpdate) {
        final String objectJSON = "securities";
        String urlStringRaw = "http://iss.moex.com/iss/engines/stock/markets/bonds/securities.json" +
                              "?iss.meta=off&iss.only=securities&securities.columns=";
        String urlColumnList = "SECID,BOARDID,SHORTNAME,PREVWAPRICE,YIELDATPREVWAPRICE,COUPONVALUE,NEXTCOUPON," +
                               "ACCRUEDINT,PREVPRICE,LOTSIZE,FACEVALUE,BOARDNAME,STATUS,MATDATE,DECIMALS," +
                               "COUPONPERIOD,ISSUESIZE,PREVLEGALCLOSEPRICE,PREVADMITTEDQUOTE,PREVDATE,SECNAME," +
                               "REMARKS,MARKETCODE,INSTRID,SECTORID,MINSTEP,FACEUNIT,BUYBACKPRICE,BUYBACKDATE," +
                               "ISIN,LATNAME,REGNUMBER,CURRENCYID,ISSUESIZEPLACED,LISTLEVEL,SECTYPE,COUPONPERCENT," +
                               "OFFERDATE,SETTLEDATE,LOTVALUE";

        String urlStringData = urlStringRaw.concat(urlColumnList);
        load(stmtUpdate, urlStringData, objectJSON);
    }

    @Override
    public StageSecurityDailyInfoBondsData accumulateData(JSONArray jsonArray) {
        StageSecurityDailyInfoBondsData data;
        data = new StageSecurityDailyInfoBondsData(jsonArray.optString(0), // SECID
                                                   jsonArray.optString(1), // BOARDID
                                                   jsonArray.optString(2), // SHORTNAME
                                                   jsonArray.optDouble(3), // PREVWAPRICE
                                                   jsonArray.optDouble(4), // YIELDATPREVWAPRICE
                                                   jsonArray.optDouble(5), // COUPONVALUE
                                                   jsonArray.optString(6), // NEXTCOUPON
                                                   jsonArray.optDouble(7), // ACCRUEDINT
                                                   jsonArray.optDouble(8), // PREVPRICE
                                                   jsonArray.optInt(9), // LOTSIZE
                                                   jsonArray.optDouble(10), // FACEVALUE
                                                   jsonArray.optString(11), // BOARDNAME
                                                   jsonArray.optString(12), // STATUS
                                                   jsonArray.optString(13), // MATDATE
                                                   jsonArray.optInt(14), // DECIMALS
                                                   jsonArray.optInt(15), // COUPONPERIOD
                                                   jsonArray.optLong(16), // ISSUESIZE
                                                   jsonArray.optDouble(17), // PREVLEGALCLOSEPRICE
                                                   jsonArray.optDouble(18), // PREVADMITTEDQUOTE
                                                   jsonArray.optString(19), // PREVDATE
                                                   jsonArray.optString(20), // SECNAME
                                                   jsonArray.optString(21), // REMARKS
                                                   jsonArray.optString(22), // MARKETCODE
                                                   jsonArray.optString(23), // INSTRID
                                                   jsonArray.optString(24), // SECTORID
                                                   jsonArray.optDouble(25), // MINSTEP
                                                   jsonArray.optString(26), // FACEUNIT
                                                   jsonArray.optDouble(27), // BUYBACKPRICE
                                                   jsonArray.optString(28), // BUYBACKDATE
                                                   jsonArray.optString(29), // ISIN
                                                   jsonArray.optString(30), // LATNAME
                                                   jsonArray.optString(31), // REGNUMBER
                                                   jsonArray.optString(32), // CURRENCYID
                                                   jsonArray.optLong(33), // ISSUESIZEPLACED
                                                   jsonArray.optInt(34), // LISTLEVEL
                                                   jsonArray.optString(35), // SECTYPE
                                                   jsonArray.optDouble(36), // COUPONPERCENT
                                                   jsonArray.optString(37), // OFFERDATE
                                                   jsonArray.optString(38), // SETTLEDATE
                                                   jsonArray.optDouble(39)); // LOTVALUE
        return data;
    }

    @Override
    public void saveData(PreparedStatement stmtUpdate, List<? extends ExternalData> dataArray) {
        try {
            for(ExternalData iter : dataArray) {
                StageSecurityDailyInfoBondsData jter = (StageSecurityDailyInfoBondsData) iter;
                stmtUpdate.setLong(1, this.getFlowLoadId());
                stmtUpdate.setString(2, this.getEntityStartLoadTimestamp().format(dateTimeFormat));
                stmtUpdate.setString(3, jter.securityId); // SECID
                stmtUpdate.setString(4, jter.boardId); // BOARDID
                stmtUpdate.setString(5, jter.shortName); // SHORTNAME
                stmtUpdate.setDouble(6, jter.previousWaPrice); // PREVWAPRICE
                stmtUpdate.setDouble(7, jter.yieldDatePrevWaPrice); // YIELDATPREVWAPRICE
                stmtUpdate.setDouble(8, jter.couponValue); // COUPONVALUE
                stmtUpdate.setString(9, jter.nextCoupon); // NEXTCOUPON
                stmtUpdate.setDouble(10, jter.accruedInt); // ACCRUEDINT
                stmtUpdate.setDouble(11, jter.previousPrice); // PREVPRICE
                stmtUpdate.setInt(12, jter.lotSize); // LOTSIZE
                stmtUpdate.setDouble(13, jter.faceValue); // FACEVALUE
                stmtUpdate.setString(14, jter.boardName); // BOARDNAME
                stmtUpdate.setString(15, jter.status); // STATUS
                stmtUpdate.setString(16, jter.matDate); // MATDATE
                stmtUpdate.setInt(17, jter.decimals); // DECIMALS
                stmtUpdate.setInt(18, jter.couponPeriod); // COUPONPERIOD
                stmtUpdate.setLong(19, jter.issueSize); // ISSUESIZE
                stmtUpdate.setDouble(20, jter.previousLegalClosePrice); // PREVLEGALCLOSEPRICE
                stmtUpdate.setDouble(21, jter.previousAdmittedQuote); // PREVADMITTEDQUOTE
                stmtUpdate.setString(22, jter.previousDate); // PREVDATE
                stmtUpdate.setString(23, jter.securityName); // SECNAME
                stmtUpdate.setString(24, jter.remarks); // REMARKS
                stmtUpdate.setString(25, jter.marketCode); // MARKETCODE
                stmtUpdate.setString(26, jter.instrId); // INSTRID
                stmtUpdate.setString(27, jter.sectorId); // SECTORID
                stmtUpdate.setDouble(28, jter.minStep); // MINSTEP
                stmtUpdate.setString(29, jter.faceUnit); // FACEUNIT
                stmtUpdate.setDouble(30, jter.buyBackPrice); // BUYBACKPRICE
                stmtUpdate.setString(31, jter.buyBackDate); // BUYBACKDATE
                stmtUpdate.setString(32, jter.isin); // ISIN
                stmtUpdate.setString(33, jter.latName); // LATNAME
                stmtUpdate.setString(34, jter.regNumber); // REGNUMBER
                stmtUpdate.setString(35, jter.currencyId); // CURRENCYID
                stmtUpdate.setLong(36, jter.issueSizePlaced); // ISSUESIZEPLACED
                stmtUpdate.setInt(37, jter.listLevel); // LISTLEVEL
                stmtUpdate.setString(38, jter.securityType); // SECTYPE
                stmtUpdate.setDouble(39, jter.couponPercent); // COUPONPERCENT
                stmtUpdate.setString(40, jter.offerDate); // OFFERDATE
                stmtUpdate.setString(41, jter.settleDate); // SETTLEDATE
                stmtUpdate.setDouble(42, jter.lotValue); // LOTVALUE
                stmtUpdate.addBatch();
            }
            setInsertCount(getInsertCount() + stmtUpdate.executeBatch().length);
            stmtUpdate.clearBatch();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
