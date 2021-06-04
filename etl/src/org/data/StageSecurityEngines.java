package org.data;

import org.flow.Flow;
import org.json.JSONArray;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

import static org.util.AuxUtil.dateTimeFormat;

public class StageSecurityEngines extends SnapshotEntity implements ImoexSourceEntity {

    static class StageSecurityEnginesData extends ExternalData {
        int    id;
        String name;
        String title;

        public StageSecurityEnginesData(int    id,
                                        String name,
                                        String title) {
            this.id = id;
            this.name = name;
            this.title = title;
        }

    }

    public StageSecurityEngines(Flow flow) {
        super(flow, "security_engines");
    }

    @Override
    public void detailLoad(PreparedStatement stmtUpdate) {
        final String objectJSON = "engines";
        String urlStringRaw = "http://iss.moex.com/iss/index.json?iss.meta=off&iss.only=engines&engines.columns=";
        String urlColumnList = "id,name,title";

        String urlStringData = urlStringRaw.concat(urlColumnList);
        saveData(stmtUpdate, load(urlStringData, objectJSON));
    }

    @Override
    public StageSecurityEnginesData accumulateData(JSONArray jsonArray) {
        StageSecurityEnginesData data;
        data = new StageSecurityEnginesData(jsonArray.optInt(0), // id
                                            jsonArray.optString(1), // name
                                            jsonArray.optString(2)); // title
        return data;
    }

    @Override
    public void saveData(PreparedStatement stmtUpdate, List<? extends ExternalData> dataArray) {
        try {
            for(ExternalData iter : dataArray) {
                StageSecurityEnginesData jter = (StageSecurityEnginesData) iter;
                stmtUpdate.setLong(1, this.getFlowLoadId());
                stmtUpdate.setString(2, this.getFlowLogStartTimestamp().format(dateTimeFormat));
                stmtUpdate.setInt(3, jter.id);
                stmtUpdate.setString(4, jter.name);
                stmtUpdate.setString(5, jter.title);
                stmtUpdate.addBatch();
            }
            setInsertCount(getInsertCount() + stmtUpdate.executeBatch().length);
            stmtUpdate.clearBatch();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
