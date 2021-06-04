package org.data;

import org.flow.Flow;
import org.json.JSONArray;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

import static org.util.AuxUtil.dateTimeFormat;

public class StageSecurityCollections extends SnapshotEntity implements ImoexSourceEntity {

    static class StageSecurityCollectionsData extends ExternalData {
        int    id;
        String name;
        String title;
        int    securityGroupId;

        public StageSecurityCollectionsData(int    id,
                                            String name,
                                            String title,
                                            int    securityGroupId) {
            this.id = id;
            this.name = name;
            this.title = title;
            this.securityGroupId = securityGroupId;
        }
    }

    public StageSecurityCollections(Flow flow) {
        super(flow, "security_collections");
    }

    @Override
    public void detailLoad(PreparedStatement stmtUpdate) {
        final String objectJSON = "securitycollections";
        String urlStringRaw = "http://iss.moex.com/iss/index.json" +
                              "?iss.meta=off&iss.only=securitycollections&securitycollections.columns=";
        String urlColumnList = "id,name,title,security_group_id";

        String urlStringData = urlStringRaw.concat(urlColumnList);
        saveData(stmtUpdate, load(urlStringData, objectJSON));
    }

    @Override
    public StageSecurityCollectionsData accumulateData(JSONArray jsonArray) {
        StageSecurityCollectionsData data;
        data = new StageSecurityCollectionsData(jsonArray.optInt(0), // id
                                                jsonArray.optString(1), // name
                                                jsonArray.optString(2), // title
                                                jsonArray.optInt(3)); // security_group_id
        return data;
    }

    @Override
    public void saveData(PreparedStatement stmtUpdate, List<? extends ExternalData> dataArray) {
        try {
            for(ExternalData iter : dataArray) {
                StageSecurityCollectionsData jter = (StageSecurityCollectionsData) iter;
                stmtUpdate.setLong(1, this.getFlowLoadId());
                stmtUpdate.setString(2, this.getFlowLogStartTimestamp().format(dateTimeFormat));
                stmtUpdate.setInt(3, jter.id);
                stmtUpdate.setString(4, jter.name);
                stmtUpdate.setString(5, jter.title);
                stmtUpdate.setInt(6, jter.securityGroupId);
                stmtUpdate.addBatch();
            }
            setInsertCount(getInsertCount() + stmtUpdate.executeBatch().length);
            stmtUpdate.clearBatch();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
