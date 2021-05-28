package org.data;

import org.json.JSONArray;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

public class StageSecurityTypes extends ImoexWebSiteEntity {

    static class StageSecurityTypesData extends ImoexData {
        int    id;
        int    tradeEngineId;
        String tradeEngineName;
        String tradeEngineTitle;
        String securityTypeName;
        String securityTypeTitle;
        String securityGroupName;


        public StageSecurityTypesData(int    id,
                                       int    tradeEngineId,
                                       String tradeEngineName,
                                       String tradeEngineTitle,
                                       String securityTypeName,
                                       String securityTypeTitle,
                                       String securityGroupName) {
            this.id = id;
            this.tradeEngineId = tradeEngineId;
            this.tradeEngineName = tradeEngineName;
            this.tradeEngineTitle = tradeEngineTitle;
            this.securityTypeName = securityTypeName;
            this.securityTypeTitle = securityTypeTitle;
            this.securityGroupName = securityGroupName;
        }

    }

    public StageSecurityTypes(long flowLoadId) {
        super(flowLoadId, "security_types");
    }

    public void detailLoad(PreparedStatement stmtUpdate) {
        String urlStringData = "http://iss.moex.com/iss/index.json?iss.meta=off&iss.only=securitytypes&securitytypes.columns=id,trade_engine_id,trade_engine_name,trade_engine_title,security_type_name,security_type_title,security_group_name";

        try {
//            completeLoad(stmtUpdate, null, urlStringData);
            singleIterationLoad(stmtUpdate, urlStringData, "securitytypes");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    StageSecurityTypesData accumulateData(JSONArray jsonArray) {
        StageSecurityTypesData data;
        data = new StageSecurityTypesData(jsonArray.optInt(0), // id
                jsonArray.optInt(1), // trade_engine_id
                jsonArray.optString(2), // trade_engine_name
                jsonArray.optString(3), // trade_engine_title
                jsonArray.optString(4), // security_type_name
                jsonArray.optString(5), // security_type_title
                jsonArray.optString(6));    // security_group_name

        return data;
    }

    void saveData(PreparedStatement stmtUpdate, List<? extends ImoexData> dataArray) {
        try {
            System.out.println("iter.count = " + dataArray.size());
            for(ImoexData iter : dataArray) {
                StageSecurityTypesData jter = (StageSecurityTypesData) iter;
//                System.out.println("jter.marketName = " + jter.marketName);
                stmtUpdate.setInt(1, jter.id);
                stmtUpdate.setInt(2, jter.tradeEngineId);
                stmtUpdate.setString(3, jter.tradeEngineName);
                stmtUpdate.setString(4, jter.tradeEngineTitle);
                stmtUpdate.setString(5, jter.securityTypeName);
                stmtUpdate.setString(6, jter.securityTypeTitle);
                stmtUpdate.setString(7, jter.securityGroupName);
                stmtUpdate.setLong(8, this.getFlowLoadId());
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
