package org.data;

import org.json.JSONArray;

import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

public class StageSecurityDailyInfo extends ImoexWebSiteEntity {

    static class StageSecurityDailyInfoData extends ImoexData {
        String     securityId;
        String     boardId;
        String     shortName;
        BigDecimal previousPrice;
        int        lotSize;
        BigDecimal faceValue;
        String     status;
        int        decimals;
        String     securityName;
        String     remarks;
        String     marketCode;
        String     instrId;
        BigDecimal minStep;
        BigDecimal prevWaPrice;
        String     faceUnit;
        String     previousDate;
        long       issueSize;
        String     isin;
        String     latName;
        String     regNumber;
        BigDecimal previousLegalClosePrice;
        BigDecimal previousAdmittedQuote;
        String     currencyId;
        String     securityType;
        int        listLevel;
        String     settleDate;


        public StageSecurityDailyInfoData(String     securityId,
                                          String     boardId,
                                          String     shortName,
                                          BigDecimal previousPrice,
                                          int        lotSize,
                                          BigDecimal faceValue,
                                          String     status,
                                          int        decimals,
                                          String     securityName,
                                          String     remarks,
                                          String     marketCode,
                                          String     instrId,
                                          BigDecimal minStep,
                                          BigDecimal prevWaPrice,
                                          String     faceUnit,
                                          String     previousDate,
                                          long       issueSize,
                                          String     isin,
                                          String     latName,
                                          String     regNumber,
                                          BigDecimal previousLegalClosePrice,
                                          BigDecimal previousAdmittedQuote,
                                          String     currencyId,
                                          String     securityType,
                                          int        listLevel,
                                          String     settleDate) {
            this.securityId = securityId;
            this.boardId = boardId;
            this.shortName = shortName;
            this.previousPrice = previousPrice;
            this.lotSize = lotSize;
            this.faceValue = faceValue;
            this.status = status;
            this.decimals = decimals;
            this.securityName = securityName;
            this.remarks = remarks;
            this.marketCode = marketCode;
            this.instrId = instrId;
            this.minStep = minStep;
            this.prevWaPrice = prevWaPrice;
            this.faceUnit = faceUnit;
            this.previousDate = previousDate;
            this.issueSize = issueSize;
            this.isin = isin;
            this.latName = latName;
            this.regNumber = regNumber;
            this.previousLegalClosePrice = previousLegalClosePrice;
            this.previousAdmittedQuote = previousAdmittedQuote;
            this.currencyId = currencyId;
            this.securityType = securityType;
            this.listLevel = listLevel;
            this.settleDate = settleDate;
        }

    }

    public StageSecurityDailyInfo(long flowLoadId) {
        super(flowLoadId, "security_daily_info"/*, loadMode*/);
    }

    public void detailLoad(PreparedStatement stmtUpdate) {
//        http://iss.moex.com/iss/engines/stock/markets/shares/securities.json?iss.meta=off&iss.only=securities&securities.columns=SECID,BOARDID,SHORTNAME,PREVPRICE,LOTSIZE,FACEVALUE,STATUS,BOARNAME,DECIMALS,SECNAME,REMARKS,MARKETCODE,INSTRID,MINSTEP,PREVWAPRICE,FACEUNIT,PREVDATE,ISSUESIZE,ISIN,LATNAME,REGNUMBER,PREVLEGALCLOSEPRICE,PREVADMITTEDQUOTE,CURRENCYID,SECTYPE,LISTLEVEL,SETTLEDATE
        String urlStringRaw = "http://iss.moex.com/iss/engines/stock/markets/";
        String urlStringData;
        String urlColumnList = "/securities.json?iss.meta=off&iss.only=securities&securities.columns=SECID,BOARDID,SHORTNAME,PREVPRICE,LOTSIZE,FACEVALUE,STATUS,BOARNAME,DECIMALS,SECNAME,REMARKS,MARKETCODE,INSTRID,MINSTEP,PREVWAPRICE,FACEUNIT,PREVDATE,ISSUESIZE,ISIN,LATNAME,REGNUMBER,PREVLEGALCLOSEPRICE,PREVADMITTEDQUOTE,CURRENCYID,SECTYPE,LISTLEVEL,SETTLEDATE";
        String urlStringMeta = null;
        urlStringData = urlStringRaw.concat("shares").concat(urlColumnList);

        try {
//            completeLoad(stmtUpdate, urlStringMeta, urlStringData);
            singleIterationLoad(stmtUpdate, urlStringData, "data");
        } catch (Exception e) {
            e.printStackTrace();
        }

        urlStringData = urlStringRaw.concat("bonds").concat(urlColumnList);

        try {
//            completeLoad(stmtUpdate, urlStringMeta, urlStringData);
            singleIterationLoad(stmtUpdate, urlStringData, "data");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    StageSecurityDailyInfoData accumulateData(JSONArray jsonArray) {
        StageSecurityDailyInfoData data;
        data = new StageSecurityDailyInfoData(jsonArray.optString(0),
                                              jsonArray.optString(1),
                                              jsonArray.optString(2),
                                                jsonArray.optBigDecimal(3, BigDecimal.ZERO),
//                                              new BigDecimal(jsonArray.optDouble(3)),
                                              jsonArray.optInt(4),
                                                jsonArray.optBigDecimal(5, BigDecimal.ZERO),
//                                              new BigDecimal(jsonArray.optString(5)),
                                              jsonArray.optString(6),
                                              jsonArray.optInt(7),
                                              jsonArray.optString(8),
                                              jsonArray.optString(9),
                                              jsonArray.optString(10),
                                              jsonArray.optString(11),
                                                jsonArray.optBigDecimal(12, BigDecimal.ZERO),
                                                jsonArray.optBigDecimal(13, BigDecimal.ZERO),
//                                              new BigDecimal(jsonArray.optString(12)),
//                                              new BigDecimal(jsonArray.optString(13)),
                                              jsonArray.optString(14),
                                              jsonArray.optString(15),
                                              jsonArray.optLong(16),
                                              jsonArray.optString(17),
                                              jsonArray.optString(18),
                                              jsonArray.optString(19),
                                                jsonArray.optBigDecimal(20, BigDecimal.ZERO),
                                                jsonArray.optBigDecimal(21, BigDecimal.ZERO),
//                                              new BigDecimal(jsonArray.optString(20)),
//                                              new BigDecimal(jsonArray.optString(21)),
                                              jsonArray.optString(22),
                                              jsonArray.optString(23),
                                              jsonArray.optInt(24),
                                              jsonArray.optString(25));
//        System.out.println(data.tradeDate);
        return data;
    }

    void saveData(PreparedStatement stmtUpdate, List<? extends ImoexData> dataArray) {
        try {
            System.out.println("iter.count = " + dataArray.size());
            for(ImoexData iter : dataArray) {
                StageSecurityDailyInfoData jter = (StageSecurityDailyInfoData) iter;
//            System.out.println(jter.tradeDate);


                stmtUpdate.setString(1, jter.securityId);
                stmtUpdate.setString(2, jter.boardId);
                stmtUpdate.setString(3, jter.shortName);
                stmtUpdate.setBigDecimal(4, jter.previousPrice);
                stmtUpdate.setInt(5, jter.lotSize);
                stmtUpdate.setBigDecimal(6, jter.faceValue);
                stmtUpdate.setString(7, jter.status);
                stmtUpdate.setInt(8, jter.decimals);
                stmtUpdate.setString(9, jter.securityName);
                stmtUpdate.setString(10, jter.remarks);
                stmtUpdate.setString(11, jter.marketCode);
                stmtUpdate.setString(12, jter.instrId);
                stmtUpdate.setBigDecimal(13, jter.minStep);
                stmtUpdate.setBigDecimal(14, jter.prevWaPrice);
                stmtUpdate.setString(15, jter.faceUnit);
                stmtUpdate.setString(16, jter.previousDate);
                stmtUpdate.setLong(17, jter.issueSize);
                stmtUpdate.setString(18, jter.isin);
                stmtUpdate.setString(19, jter.latName);
                stmtUpdate.setString(20, jter.regNumber);
                stmtUpdate.setBigDecimal(21, jter.previousLegalClosePrice);
                stmtUpdate.setBigDecimal(22, jter.previousAdmittedQuote);
                stmtUpdate.setString(23, jter.currencyId);
                stmtUpdate.setString(24, jter.securityType);
                stmtUpdate.setInt(25, jter.listLevel);
                stmtUpdate.setString(26, jter.settleDate);
                stmtUpdate.setLong(27, this.getFlowLoadId());
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
