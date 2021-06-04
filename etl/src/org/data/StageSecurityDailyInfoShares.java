package org.data;

import org.flow.Flow;
import org.json.JSONArray;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

import static org.util.AuxUtil.dateTimeFormat;

public class StageSecurityDailyInfoShares extends SnapshotEntity implements ImoexSourceEntity {

    static class StageSecurityDailyInfoSharesData extends ExternalData {
        String securityId;
        String boardId;
        String shortName;
        double previousPrice;
        int    lotSize;
        double faceValue;
        String status;
        String boardName;
        int    decimals;
        String securityName;
        String remarks;
        String marketCode;
        String instrId;
        double minStep;
        String sectorId;
        double prevWaPrice;
        String faceUnit;
        String previousDate;
        long   issueSize;
        String isin;
        String latName;
        String regNumber;
        double previousLegalClosePrice;
        double previousAdmittedQuote;
        String currencyId;
        String securityType;
        int    listLevel;
        String settleDate;


        public StageSecurityDailyInfoSharesData(String securityId,
                                                String boardId,
                                                String shortName,
                                                double previousPrice,
                                                int    lotSize,
                                                double faceValue,
                                                String status,
                                                String boardName,
                                                int    decimals,
                                                String securityName,
                                                String remarks,
                                                String marketCode,
                                                String instrId,
                                                double minStep,
                                                String sectorId,
                                                double prevWaPrice,
                                                String faceUnit,
                                                String previousDate,
                                                long   issueSize,
                                                String isin,
                                                String latName,
                                                String regNumber,
                                                double previousLegalClosePrice,
                                                double previousAdmittedQuote,
                                                String currencyId,
                                                String securityType,
                                                int    listLevel,
                                                String settleDate) {
            this.securityId = securityId;
            this.boardId = boardId;
            this.shortName = shortName;
            this.previousPrice = previousPrice;
            this.lotSize = lotSize;
            this.faceValue = faceValue;
            this.status = status;
            this.boardName = boardName;
            this.decimals = decimals;
            this.securityName = securityName;
            this.remarks = remarks;
            this.marketCode = marketCode;
            this.instrId = instrId;
            this.minStep = minStep;
            this.sectorId = sectorId;
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

    public StageSecurityDailyInfoShares(Flow flow) {
        super(flow, "security_daily_info_shares");
    }

    @Override
    public void detailLoad(PreparedStatement stmtUpdate) {
        final String objectJSON = "securities";
        String urlStringRaw = "http://iss.moex.com/iss/engines/stock/markets/shares/securities.json" +
                              "?iss.meta=off&iss.only=securities&securities.columns=";
        String urlColumnList = "SECID,BOARDID,SHORTNAME,PREVPRICE,LOTSIZE,FACEVALUE,STATUS,BOARDNAME,DECIMALS," +
                               "SECNAME,REMARKS,MARKETCODE,INSTRID,MINSTEP,PREVWAPRICE,FACEUNIT,PREVDATE,ISSUESIZE," +
                               "ISIN,LATNAME,REGNUMBER,PREVLEGALCLOSEPRICE,PREVADMITTEDQUOTE,CURRENCYID,SECTYPE," +
                               "LISTLEVEL,SETTLEDATE";

        String urlStringData = urlStringRaw.concat(urlColumnList);
        saveData(stmtUpdate, load(urlStringData, objectJSON));
    }

    @Override
    public StageSecurityDailyInfoSharesData accumulateData(JSONArray jsonArray) {
        StageSecurityDailyInfoSharesData data;
        data = new StageSecurityDailyInfoSharesData(jsonArray.optString(0), // SECID
                                                    jsonArray.optString(1), // BOARDID
                                                    jsonArray.optString(2), // SHORTNAME
                                                    jsonArray.optDouble(3), // PREVPRICE
                                                    jsonArray.optInt(4), // LOTSIZE
                                                    jsonArray.optDouble(5), // FACEVALUE
                                                    jsonArray.optString(6), // STATUS
                                                    jsonArray.optString(7), // BOARDNAME
                                                    jsonArray.optInt(8), // DECIMALS
                                                    jsonArray.optString(9), // SECNAME
                                                    jsonArray.optString(10), // REMARKS
                                                    jsonArray.optString(11), // MARKETCODE
                                                    jsonArray.optString(12), // INSTRID
                                                    jsonArray.optDouble(13), // MINSTEP
                                                    jsonArray.optString(14), // SECTORID
                                                    jsonArray.optDouble(15), // PREVWAPRICE
                                                    jsonArray.optString(16), // FACEUNIT
                                                    jsonArray.optString(17), // PREVDATE
                                                    jsonArray.optLong(18), // ISSUESIZE
                                                    jsonArray.optString(19), // ISIN
                                                    jsonArray.optString(20), // LATNAME
                                                    jsonArray.optString(21), // REGNUMBER
                                                    jsonArray.optDouble(22), // PREVLEGALCLOSEPRICE
                                                    jsonArray.optDouble(23), // PREVADMITTEDQUOTE
                                                    jsonArray.optString(24), // CURRENCYID
                                                    jsonArray.optString(25), // SECTYPE
                                                    jsonArray.optInt(26), // LISTLEVEL
                                                    jsonArray.optString(27)); // SETTLEDATE
        return data;
    }

    @Override
    public void saveData(PreparedStatement stmtUpdate, List<? extends ExternalData> dataArray) {
        try {
            for(ExternalData iter : dataArray) {
                StageSecurityDailyInfoSharesData jter = (StageSecurityDailyInfoSharesData) iter;
                stmtUpdate.setLong(1, this.getFlowLoadId());
                stmtUpdate.setString(2, this.getFlowLogStartTimestamp().format(dateTimeFormat));
                stmtUpdate.setString(3, jter.securityId);
                stmtUpdate.setString(4, jter.boardId);
                stmtUpdate.setString(5, jter.shortName);
                stmtUpdate.setDouble(6, jter.previousPrice);
                stmtUpdate.setInt(7, jter.lotSize);
                stmtUpdate.setDouble(8, jter.faceValue);
                stmtUpdate.setString(9, jter.status);
                stmtUpdate.setInt(10, jter.decimals);
                stmtUpdate.setString(11, jter.boardName);
                stmtUpdate.setString(12, jter.securityName);
                stmtUpdate.setString(13, jter.remarks);
                stmtUpdate.setString(14, jter.marketCode);
                stmtUpdate.setString(15, jter.instrId);
                stmtUpdate.setDouble(16, jter.minStep);
                stmtUpdate.setString(17, jter.sectorId);
                stmtUpdate.setDouble(18, jter.prevWaPrice);
                stmtUpdate.setString(19, jter.faceUnit);
                stmtUpdate.setString(20, jter.previousDate);
                stmtUpdate.setLong(21, jter.issueSize);
                stmtUpdate.setString(22, jter.isin);
                stmtUpdate.setString(23, jter.latName);
                stmtUpdate.setString(24, jter.regNumber);
                stmtUpdate.setDouble(25, jter.previousLegalClosePrice);
                stmtUpdate.setDouble(26, jter.previousAdmittedQuote);
                stmtUpdate.setString(27, jter.currencyId);
                stmtUpdate.setString(28, jter.securityType);
                stmtUpdate.setInt(29, jter.listLevel);
                stmtUpdate.setString(30, jter.settleDate);
                stmtUpdate.addBatch();
            }
            setInsertCount(getInsertCount() + stmtUpdate.executeBatch().length);
            stmtUpdate.clearBatch();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
