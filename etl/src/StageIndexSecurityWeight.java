import org.json.JSONArray;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;

public class StageIndexSecurityWeight implements EntityRelation {

    static class IndexSecurityData {

        private String indexName;
        private String tradeDate;
        private String securityId;
        private Double weight;

        public IndexSecurityData(String indexName, String tradeDate, String securityId, Double weight) {
            this.indexName = indexName;
            this.tradeDate = tradeDate;
            this.securityId = securityId;
            this.weight = weight;
        }

        public String getIndexName() {
            return this.indexName;
        }
        public String getTradeDate() {
            return this.tradeDate;
        }
        public String getSecurityId() {
            return this.securityId;
        }
        public Double getWeight() {
            return this.weight;
        }

    }

    private int pageSize;
    private int pageCount;

    private String effectiveFromDate = "";
    private String effectiveToDate = "";
    private String previousDate = null;
    private String currentDate = null;

    private List<IndexSecurityData> indexSecurityArray = new ArrayList<>();

    private final String sqlInsertString = "INSERT INTO index_security_weight(index_name, trade_date, secid, weight, load_id) VALUES(?, ?, ?, ?, ?)";

    private void loadMetaInfo() throws Exception {

        String urlString = "http://iss.moex.com/iss/statistics/engines/stock/markets/index/analytics/IMOEX.json?iss.meta=off&iss.only=analytics.cursor&analytics.cursor.columns=TOTAL,PAGESIZE,NEXT_DATE";
//        System.out.println(previousDate == null ? "2021-04-01" : previousDate);
        URL url = new URL(urlString.concat("&date=" + (previousDate == null ? "2021-04-01" : previousDate)));
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("GET");

        int status = conn.getResponseCode();
        try(BufferedReader in = new BufferedReader(new InputStreamReader(conn.getInputStream()))) {
            String inputLine;
            StringBuffer content = new StringBuffer();

            while ((inputLine = in.readLine()) != null) {
                content.append(inputLine);
            }

            Iterator<Object> iter = new JSONObject(content.toString())
                    .optJSONObject("analytics.cursor")
                    .getJSONArray("data").iterator();

            while (iter.hasNext()) {
                JSONArray test = (JSONArray) iter.next();
                pageSize = test.optInt(1);
                pageCount = (int) Math.ceil(test.optDouble(0)/test.optDouble(1));
                currentDate = test.optString(2);
            }
        }
        conn.disconnect();
    }

    private void loadData() throws Exception {
        String urlString = "http://iss.moex.com/iss/statistics/engines/stock/markets/index/analytics/IMOEX.json?iss.meta=off&iss.only=analytics&analytics.columns=indexid,tradedate,secids,weight";

        for (int ind = 0; ind < pageCount; ind++) {
            URL url = new URL(urlString.concat("&date=" + currentDate).concat("&start=" + String.valueOf(ind * pageSize)));
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("GET");

            int status = conn.getResponseCode();
            try (BufferedReader in = new BufferedReader(new InputStreamReader(conn.getInputStream()))) {
                Map<String, Double> inMap = new HashMap<>();
                String inputLine;
                StringBuffer content = new StringBuffer();

                while ((inputLine = in.readLine()) != null) {
                    content.append(inputLine);
                }

                Iterator<Object> iter = new JSONObject(content.toString())
                        .optJSONObject("analytics")
                        .getJSONArray("data").iterator();

                while (iter.hasNext()) {
                    JSONArray test = (JSONArray) iter.next();
                    indexSecurityArray.add(new IndexSecurityData(test.optString(0), test.optString(1), test.optString(2), test.optDouble(3)));
                }
            }
            conn.disconnect();
        }
    }

    private void saveData(int entityLoadId) throws Exception {

        String sqlString = "INSERT INTO index_security_weight(index_name, trade_date, secid, weight, tech$load_id) VALUES(?, ?, ?, ?, ?)";
        try(Connection conn = DriverManager.getConnection("jdbc:sqlite:../db/file/stage.db");
            PreparedStatement stmt = conn.prepareStatement(sqlString)) {

            for(IndexSecurityData iter : indexSecurityArray) {
                stmt.setString(1, iter.getIndexName());
                stmt.setString(2, iter.getTradeDate());
                stmt.setString(3, iter.getSecurityId());
                stmt.setDouble(4, iter.getWeight());
                stmt.setInt(5, entityLoadId);
                stmt.addBatch();
            }
            stmt.executeBatch();

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }


    public QueueElement load(QueueElement queueElement) {
        QueueRelationElement returnQueueRelationElement = null;
        if (queueElement instanceof QueueRelationElement) {
            returnQueueRelationElement = new QueueRelationElement((QueueRelationElement) queueElement);
        }
        if (returnQueueRelationElement.getEntityLoadMode().equals("incr")) {
            previousDate = returnQueueRelationElement.getEntityEffFromDt();
        }
        try {
            loadMetaInfo();
            while (!currentDate.isEmpty()) {
                effectiveFromDate = effectiveFromDate.isEmpty() ? currentDate : effectiveFromDate;
                loadData();
                previousDate = currentDate;
                loadMetaInfo();
            }
            effectiveToDate = previousDate;
            System.out.println(effectiveFromDate);
            System.out.println(effectiveToDate);
            saveData(queueElement.getElmId());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        returnQueueRelationElement.setEntityEffFromDt(effectiveFromDate);
        returnQueueRelationElement.setEntityEffToDt(effectiveToDate);
        return returnQueueRelationElement;
    }

/*    public static void main(String args[]) {
        new StageIndexSecurityWeight().loadIncr(2, "2021-04-01");
    }*/
}
