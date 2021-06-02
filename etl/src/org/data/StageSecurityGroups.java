package org.data;

import org.json.JSONArray;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

public class StageSecurityGroups extends ImoexWebSiteEntity {

    static class StageSecurityGroupsData extends ImoexData {
        int    id;
        String name;
        String title;
        int    isHidden;


        public StageSecurityGroupsData(int    id,
                                        String name,
                                        String title,
                                        int    isHidden) {
            this.id = id;
            this.name = name;
            this.title = title;
            this.isHidden = isHidden;
        }

    }

    public StageSecurityGroups(long flowLoadId) {
        super(flowLoadId, "security_groups");
    }

    public void detailLoad(PreparedStatement stmtUpdate) {
        String urlStringData = "http://iss.moex.com/iss/index.json?iss.meta=off&iss.only=securitygroups&securitygroups.columns=id,name,title,is_hidden";

        try {
//            completeLoad(stmtUpdate, null, urlStringData);
//            singleIterationLoad(stmtUpdate, urlStringData, "securitygroups");
            noHistoryLoad(stmtUpdate, urlStringData, "securitygroups", false);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    StageSecurityGroupsData accumulateData(JSONArray jsonArray) {
        StageSecurityGroupsData data;
        data = new StageSecurityGroupsData(jsonArray.optInt(0),
                                            jsonArray.optString(1),
                                            jsonArray.optString(2),
                                            jsonArray.optInt(3));
        return data;
    }

    void saveData(PreparedStatement stmtUpdate, List<? extends ImoexData> dataArray) {
        try {
            System.out.println("iter.count = " + dataArray.size());
            for(ImoexData iter : dataArray) {
                StageSecurityGroupsData jter = (StageSecurityGroupsData) iter;

                stmtUpdate.setInt(1, jter.id);
                stmtUpdate.setString(2, jter.name);
                stmtUpdate.setString(3, jter.title);
                stmtUpdate.setInt(4, jter.isHidden);
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
