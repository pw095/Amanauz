package org.data;

import org.flow.Flow;
import org.json.JSONArray;
import org.meta.MetaLayer;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

import static org.util.AuxUtil.dateTimeFormat;

public class StageSecurityTypes extends SnapshotEntity implements ImoexSourceEntity {

    static class StageSecurityTypesData extends ExternalData {
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

    public StageSecurityTypes(Flow flow) {
        super(flow, MetaLayer.STAGE, "security_types");
    }

    @Override
    public void callLoad(Connection conn) {
        concreteLoad(conn);
    }

    @Override
    public void detailLoad(PreparedStatement stmtUpdate) {
        final String objectJSON = "securitytypes";
        String urlStringRaw = "http://iss.moex.com/iss/index.json" +
                              "?iss.meta=off&iss.only=securitytypes&securitytypes.columns=";
        String urlColumnList = "id,trade_engine_id,trade_engine_name,trade_engine_title,security_type_name," +
                               "security_type_title,security_group_name";

        String urlStringData = urlStringRaw.concat(urlColumnList);
        load(stmtUpdate, urlStringData, objectJSON);
    }

    @Override
    public StageSecurityTypesData accumulateData(JSONArray jsonArray) {
        StageSecurityTypesData data;
        data = new StageSecurityTypesData(jsonArray.optInt(0), // id
                                          jsonArray.optInt(1), // trade_engine_id
                                          jsonArray.optString(2), // trade_engine_name
                                          jsonArray.optString(3), // trade_engine_title
                                          jsonArray.optString(4), // security_type_name
                                          jsonArray.optString(5), // security_type_title
                                          jsonArray.optString(6)); // security_group_name

        return data;
    }

    @Override
    public void saveData(PreparedStatement stmtUpdate, List<? extends ExternalData> dataArray) {
        try {
            System.out.println("iter.count = " + dataArray.size());
            for(ExternalData iter : dataArray) {
                StageSecurityTypesData jter = (StageSecurityTypesData) iter;
                stmtUpdate.setLong(1, this.getFlowLoadId());
                stmtUpdate.setString(2, this.getFlowLogStartTimestamp().format(dateTimeFormat));
                stmtUpdate.setInt(3, jter.id);
                stmtUpdate.setInt(4, jter.tradeEngineId);
                stmtUpdate.setString(5, jter.tradeEngineName);
                stmtUpdate.setString(6, jter.tradeEngineTitle);
                stmtUpdate.setString(7, jter.securityTypeName);
                stmtUpdate.setString(8, jter.securityTypeTitle);
                stmtUpdate.setString(9, jter.securityGroupName);
                stmtUpdate.addBatch();
            }
            setInsertCount(getInsertCount() + stmtUpdate.executeBatch().length);
            stmtUpdate.clearBatch();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
