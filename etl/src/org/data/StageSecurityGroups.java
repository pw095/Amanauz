package org.data;

import org.flow.Flow;
import org.json.JSONArray;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

import static org.util.AuxUtil.dateTimeFormat;

public class StageSecurityGroups extends SnapshotEntity implements ImoexSourceEntity {

    static class StageSecurityGroupsData extends ExternalData {
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

    public StageSecurityGroups(Flow flow) {
        super(flow, "security_groups");
    }

    @Override
    public void detailLoad(PreparedStatement stmtUpdate) {
        final String objectJSON = "securitygroups";
        String urlStringRaw = "http://iss.moex.com/iss/index.json" +
                              "?iss.meta=off&iss.only=securitygroups&securitygroups.columns=";
        String urlColumnList = "id,name,title,is_hidden";

        String urlStringData = urlStringRaw.concat(urlColumnList);
        saveData(stmtUpdate, load(urlStringData, objectJSON));
    }

    @Override
    public StageSecurityGroupsData accumulateData(JSONArray jsonArray) {
        StageSecurityGroupsData data;
        data = new StageSecurityGroupsData(jsonArray.optInt(0), // id
                                           jsonArray.optString(1), // name
                                           jsonArray.optString(2), // title
                                           jsonArray.optInt(3)); // is_hidden
        return data;
    }

    @Override
    public void saveData(PreparedStatement stmtUpdate, List<? extends ExternalData> dataArray) {
        try {
            for(ExternalData iter : dataArray) {
                StageSecurityGroupsData jter = (StageSecurityGroupsData) iter;

                stmtUpdate.setLong(1, this.getFlowLoadId());
                stmtUpdate.setString(2, this.getFlowLogStartTimestamp().format(dateTimeFormat));
                stmtUpdate.setInt(3, jter.id);
                stmtUpdate.setString(4, jter.name);
                stmtUpdate.setString(5, jter.title);
                stmtUpdate.setInt(6, jter.isHidden);
                stmtUpdate.addBatch();
            }
            setInsertCount(getInsertCount() + stmtUpdate.executeBatch().length);
            stmtUpdate.clearBatch();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
