package org.data;

import org.json.JSONArray;

import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

public class StageSecurityEngines extends ImoexWebSiteEntity {

    static class StageSecurityEnginesData extends ImoexData {
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

    public StageSecurityEngines(long flowLoadId) {
        super(flowLoadId, "security_engines");
    }

    public void detailLoad(PreparedStatement stmtUpdate) {
        String urlStringData = "http://iss.moex.com/iss/index.json?iss.meta=off&iss.only=engines&engines.columns=id,name,title";

        try {
//            completeLoad(stmtUpdate, null, urlStringData);
            noHistoryLoad(stmtUpdate, urlStringData, "engines", false);
//            singleIterationLoad(stmtUpdate, urlStringData, "engines");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    StageSecurityEnginesData accumulateData(JSONArray jsonArray) {
        StageSecurityEnginesData data;
        data = new StageSecurityEnginesData(jsonArray.optInt(0),
                                            jsonArray.optString(1),
                                            jsonArray.optString(2));
        return data;
    }

    void saveData(PreparedStatement stmtUpdate, List<? extends ImoexData> dataArray) {
        try {
            System.out.println("iter.count = " + dataArray.size());
            for(ImoexData iter : dataArray) {
                StageSecurityEnginesData jter = (StageSecurityEnginesData) iter;

                stmtUpdate.setInt(1, jter.id);
                stmtUpdate.setString(2, jter.name);
                stmtUpdate.setString(3, jter.title);
                stmtUpdate.setLong(4, this.getFlowLoadId());
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
