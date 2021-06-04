package org.data;

import org.flow.Flow;
import org.json.JSONArray;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

import static org.util.AuxUtil.dateTimeFormat;

public class StageSecurityEmitentMap extends SnapshotEntity implements ImoexSourceEntity {

    static class StageSecurityEmitentMapData extends ExternalData {
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

    public StageSecurityEmitentMap(Flow flow) {
        super(flow, "security_emitent_map");
    }

    @Override
    public void detailLoad(PreparedStatement stmtUpdate) {
        final String objectJSON = "securities";
        String urlStringRaw = "http://iss.moex.com/iss/securities.json?iss.meta=off&engine=stock&market=";
        String urlColumnList = "&securities.columns=id,secid,shortname,regnumber,name,isin,is_traded,emitent_id," +
                                "emitent_title,emitent_inn,emitent_okpo,gosreg,type,group,primary_boardid," +
                                "marketprice_boardid";

        String urlStringData;
        urlStringData = urlStringRaw.concat("shares").concat(urlColumnList);
        saveData(stmtUpdate, mutliLoad(urlStringData, objectJSON));

        urlStringData = urlStringRaw.concat("bonds").concat(urlColumnList);
        saveData(stmtUpdate, mutliLoad(urlStringData, objectJSON));

    }

    @Override
    public StageSecurityEmitentMapData accumulateData(JSONArray jsonArray) {
        StageSecurityEmitentMapData data;
        data = new StageSecurityEmitentMapData(jsonArray.optInt(0),   // id
                                               jsonArray.optString(1),// secid
                                               jsonArray.optString(2),// shortname
                                               jsonArray.optString(3),// regnumber
                                               jsonArray.optString(4),// name
                                               jsonArray.optString(5),// isin
                                               jsonArray.optInt(6),   // is_traded
                                               jsonArray.optInt(7),   // emitent_id
                                               jsonArray.optString(8),// emitent_title
                                               jsonArray.optString(9),// emitent_inn
                                               jsonArray.optString(10),// emitent_okpo
                                               jsonArray.optString(11), // gosreg
                                               jsonArray.optString(12), // type
                                               jsonArray.optString(13), // group
                                               jsonArray.optString(14), // primary_boardid
                                               jsonArray.optString(15)); // marketprice_boardid
        return data;
    }

    @Override
    public void saveData(PreparedStatement stmtUpdate, List<? extends ExternalData> dataArray) {
        try {
            for(ExternalData iter : dataArray) {
                StageSecurityEmitentMapData jter = (StageSecurityEmitentMapData) iter;

                stmtUpdate.setLong(1, this.getFlowLoadId());
                stmtUpdate.setString(2, this.getFlowLogStartTimestamp().format(dateTimeFormat));
                stmtUpdate.setInt(3, jter.id);
                stmtUpdate.setString(4, jter.securityId);
                stmtUpdate.setString(5, jter.shortName);
                stmtUpdate.setString(6, jter.regNumber);
                stmtUpdate.setString(7, jter.securityName);
                stmtUpdate.setString(8, jter.isin);
                stmtUpdate.setInt(9, jter.securityIsTraded);
                stmtUpdate.setInt(10, jter.emitentId);
                stmtUpdate.setString(11, jter.emitentTitle);
                stmtUpdate.setString(12, jter.emitentInn);
                stmtUpdate.setString(13, jter.emitentOkpo);
                stmtUpdate.setString(14, jter.emitentGosReg);
                stmtUpdate.setString(15, jter.securityType);
                stmtUpdate.setString(16, jter.securityGroup);
                stmtUpdate.setString(17, jter.primaryBoardId);
                stmtUpdate.setString(18, jter.marketPriceBoardId);
                stmtUpdate.addBatch();
            }
            setInsertCount(getInsertCount() + stmtUpdate.executeBatch().length);
            stmtUpdate.clearBatch();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
