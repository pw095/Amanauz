package org.data;

import org.json.JSONArray;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

public class StageSecurityCollections extends ImoexWebSiteEntity {

    static class StageSecurityCollectionsData extends ImoexData {
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

    public StageSecurityCollections(long flowLoadId) {
        super(flowLoadId, "security_collections");
    }

    public void detailLoad(PreparedStatement stmtUpdate) {
        String urlStringData = "http://iss.moex.com/iss/index.json?iss.meta=off&iss.only=securitycollections&securitycollections.columns=id,name,title,security_group_id";

        try {
//            completeLoad(stmtUpdate, null, urlStringData);
//            singleIterationLoad(stmtUpdate, urlStringData, "securitycollections");
            noHistoryLoad(stmtUpdate, urlStringData, "securitycollections", false);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    StageSecurityCollectionsData accumulateData(JSONArray jsonArray) {
        StageSecurityCollectionsData data;
        data = new StageSecurityCollectionsData(jsonArray.optInt(0),
                                            jsonArray.optString(1),
                                            jsonArray.optString(2),
                                            jsonArray.optInt(3));
        return data;
    }

    void saveData(PreparedStatement stmtUpdate, List<? extends ImoexData> dataArray) {
        try {
            System.out.println("iter.count = " + dataArray.size());
            for(ImoexData iter : dataArray) {
                StageSecurityCollectionsData jter = (StageSecurityCollectionsData) iter;

                stmtUpdate.setInt(1, jter.id);
                stmtUpdate.setString(2, jter.name);
                stmtUpdate.setString(3, jter.title);
                stmtUpdate.setInt(4, jter.securityGroupId);
                stmtUpdate.setLong(5, this.getFlowLoadId());
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
