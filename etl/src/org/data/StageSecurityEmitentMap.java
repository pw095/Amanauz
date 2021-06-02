package org.data;

import org.json.JSONArray;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

public class StageSecurityEmitentMap extends ImoexWebSiteEntity {

    static class StageSecurityEmitentMapData extends ImoexData {
        int    id;
        String securityId;
        String shortName;
        String regNumber;
        String securityName;
        String isin;
        int    securityIsTraded;
        int    emitentId;
        String emitentTitle;
        String emitentInn;
        String emitentOkpo;
        String emitentGosReg;
        String securityType;
        String securityGroup;
        String primaryBoardId;
        String marketPriceBoardId;


        public StageSecurityEmitentMapData(int    id,
                                           String securityId,
                                           String shortName,
                                           String regNumber,
                                           String securityName,
                                           String isin,
                                           int    securityIsTraded,
                                           int    emitentId,
                                           String emitentTitle,
                                           String emitentInn,
                                           String emitentOkpo,
                                           String emitentGosReg,
                                           String securityType,
                                           String securityGroup,
                                           String primaryBoardId,
                                           String marketPriceBoardId) {
            this.id = id;
            this.securityId = securityId;
            this.shortName = shortName;
            this.regNumber = regNumber;
            this.securityName = securityName;
            this.isin = isin;
            this.securityIsTraded = securityIsTraded;
            this.emitentId = emitentId;
            this.emitentTitle = emitentTitle;
            this.emitentInn = emitentInn;
            this.emitentOkpo = emitentOkpo;
            this.emitentGosReg = emitentGosReg;
            this.securityType = securityType;
            this.securityGroup = securityGroup;
            this.primaryBoardId = primaryBoardId;
            this.marketPriceBoardId = marketPriceBoardId;
        }

    }

    public StageSecurityEmitentMap(long flowLoadId) {
        super(flowLoadId, "security_emitent_map"/*, loadMode*/);
    }

    public void detailLoad(PreparedStatement stmtUpdate) {
//        &securities.columns=id,secid,shortname,regnumber,name,isin,is_traded,emitent_id,emitent_title,emitent_inn,emitent_okpo,gosreg,type,group,primary_boardid,marketprice_boardid&start=100
        String urlStringRaw = "http://iss.moex.com/iss/securities.json?iss.meta=off&engine=stock&market=";
        String urlStringData;
        String urlColumnList = "&securities.columns=id,secid,shortname,regnumber,name,isin,is_traded,emitent_id,emitent_title,emitent_inn,emitent_okpo,gosreg,type,group,primary_boardid,marketprice_boardid";
        String urlStringMeta = null;
        urlStringData = urlStringRaw.concat("shares").concat(urlColumnList);

        try {
            noHistoryLoad(stmtUpdate, urlStringData, "securities", true);
//            multipleIterationsLoad(stmtUpdate, urlStringData, "securities");
//            completeLoad(stmtUpdate, urlStringMeta, urlStringData);
        } catch (Exception e) {
            e.printStackTrace();
        }

        urlStringData = urlStringRaw.concat("bonds").concat(urlColumnList);

        try {
            noHistoryLoad(stmtUpdate, urlStringData, "securities", true);
//            multipleIterationsLoad(stmtUpdate, urlStringData, "securities");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    StageSecurityEmitentMapData accumulateData(JSONArray jsonArray) {
        StageSecurityEmitentMapData data;
        data = new StageSecurityEmitentMapData(jsonArray.optInt(0),
                                               jsonArray.optString(1),
                                               jsonArray.optString(2),
                                               jsonArray.optString(3),
                                               jsonArray.optString(4),
                                               jsonArray.optString(5),
                                               jsonArray.optInt(6),
                                               jsonArray.optInt(7),
                                               jsonArray.optString(8),
                                               jsonArray.optString(9),
                                               jsonArray.optString(10),
                                               jsonArray.optString(11),
                                               jsonArray.optString(12),
                                               jsonArray.optString(13),
                                               jsonArray.optString(14),
                                               jsonArray.optString(15));
//        System.out.println(data.tradeDate);
        return data;
    }

    void saveData(PreparedStatement stmtUpdate, List<? extends ImoexData> dataArray) {
        try {
            System.out.println("iter.count = " + dataArray.size());
            for(ImoexData iter : dataArray) {
                StageSecurityEmitentMapData jter = (StageSecurityEmitentMapData) iter;
//            System.out.println(jter.tradeDate);
                stmtUpdate.setInt(1, jter.id);
                stmtUpdate.setString(2, jter.securityId);
                stmtUpdate.setString(3, jter.shortName);
                stmtUpdate.setString(4, jter.regNumber);
                stmtUpdate.setString(5, jter.securityName);
                stmtUpdate.setString(6, jter.isin);
                stmtUpdate.setInt(7, jter.securityIsTraded);
                stmtUpdate.setInt(8, jter.emitentId);
                stmtUpdate.setString(9, jter.emitentTitle);
                stmtUpdate.setString(10, jter.emitentInn);
                stmtUpdate.setString(11, jter.emitentOkpo);
                stmtUpdate.setString(12, jter.emitentGosReg);
                stmtUpdate.setString(13, jter.securityType);
                stmtUpdate.setString(14, jter.securityGroup);
                stmtUpdate.setString(15, jter.primaryBoardId);
                stmtUpdate.setString(16, jter.marketPriceBoardId);
                stmtUpdate.setLong(17, this.getFlowLoadId());
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
