import org.json.JSONArray;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.*;

public class StageIndexSecurityWeight implements Entity {

    static class Data {

        private String indexName;
        private String tradeDate;
        private String securityId;
        private Double weight;

        public Data(String indexName, String tradeDate, String securityId, Double weight) {
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

    private String previousDate = null;
    private String currentDate = null;

    private List<Data> indexArrayData = new ArrayList<>();

    private final String sqlInsertString = "INSERT INTO index_security_data(index_name, trade_date, secid, weight, load_id) VALUES(?, ?, ?, ?, ?)";

    private void loadDateInfo() throws Exception {

    }

    private void loadMetaInfo() throws Exception {

        String urlString = "http://iss.moex.com/iss/statistics/engines/stock/markets/index/analytics/IMOEX.json?iss.meta=off&iss.only=analytics.cursor&analytics.cursor.columns=TOTAL,PAGESIZE,PREV_DATE";
        URL url = new URL(urlString.concat(currentDate == null ? "" : "&date=" + currentDate));
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
                previousDate = test.optString(2);
            }
        }
        conn.disconnect();
    }

    private void load() throws Exception {

        String urlString = "http://iss.moex.com/iss/statistics/engines/stock/markets/index/analytics/IMOEX.json?iss.meta=off&iss.only=analytics&analytics.columns=indexid,tradedate,secids,weight";

        for(int ind=0; ind < pageCount; ind++) {
            URL url = new URL(urlString.concat(currentDate == null ? "" : "&date=" + currentDate).concat("&start=" + String.valueOf(ind*pageSize)));
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("GET");

            int status = conn.getResponseCode();
            try(BufferedReader in = new BufferedReader(new InputStreamReader(conn.getInputStream()))) {
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
                    indexArrayData.add(new IndexSecurityData(test.optString(0), test.optString(1), test.optString(2), test.optDouble(3)));
                }
            }
            conn.disconnect();
        }
    }

    public void loadFull() {

    }

    public void loadIncr(String effectiveFromDttm) {
        currentDate = effectiveFromDttm;
    }
}
