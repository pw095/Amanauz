package org.data;

import org.json.JSONArray;
import org.json.JSONObject;
import org.meta.LoadMode;
import org.meta.MetaLayer;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.sql.PreparedStatement;
import java.text.DateFormat;
import java.time.LocalDate;
import java.time.Month;
import java.util.*;

import static org.util.AuxUtil.dateFormat;

public abstract class ImoexWebSiteEntity extends AbstractEntity {

    private static class ImoexMetaData {
        String loadDate;
        int pageSize;
        int pageCount;
    }
    static abstract class ImoexData {}

    private List<ImoexMetaData> imoexMetaDataArray = new ArrayList<>();
    private List<ImoexData> imoexDataArray = new ArrayList<>();
    String previousEffectiveToDt;
    String effectiveFromDt;
    String effectiveToDt;

    public String getPreviousEffectiveToDt() {
        return this.previousEffectiveToDt;
    }
    public void setPreviousEffectiveToDt(String previousEffectiveToDt) {
        this.previousEffectiveToDt = previousEffectiveToDt;
    }

    public String getEffectiveFromDt() {
        return this.effectiveFromDt;
    }
    public void setEffectiveFromDt(String effectiveFromDt) {
        this.effectiveFromDt = effectiveFromDt;
    }

    public String getEffectiveToDt() {
        return this.effectiveToDt;
    }
    public void setEffectiveToDt(String effectiveToDt) {
        this.effectiveToDt = effectiveToDt;
    }

    private int pageSize;
    private int pageCount;

    public ImoexWebSiteEntity(long flowLoadId, String entityCode) {
        super(flowLoadId, MetaLayer.STAGE, entityCode);
        previousEffectiveToDt = LocalDate.of(2021, Month.MAY, 01).format(dateFormat);
//        this.entityLoadMode = LoadMode.valueOf(loadMode.toUpperCase());
        /*if (entityLoadMode == LoadMode.FULL) {
            previousEffectiveToDt = LocalDate.of(1900, Month.JANUARY, 01);
        } else {
            //previousEffectiveToDt = ;
        }*/
    }

    private void loadMetaData(String urlString) throws Exception {
//        String urlString = "http://iss.moex.com/iss/statistics/engines/stock/markets/index/analytics/IMOEX.json?iss.meta=off&iss.only=analytics.cursor&analytics.cursor.columns=TOTAL,PAGESIZE,NEXT_DATE";

        String currentLoadDate = previousEffectiveToDt;
        String nextLoadDate = "";
        long iterationCount = 0;

        for(; !nextLoadDate.isEmpty() || iterationCount == 0; currentLoadDate = nextLoadDate, ++iterationCount) {
            URL url = new URL(urlString.concat("&date=" + currentLoadDate));
//            System.out.println(iterationCount);
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("GET");

            int status = conn.getResponseCode();

            try (BufferedReader in = new BufferedReader(new InputStreamReader(conn.getInputStream()))) {
                String inputLine;
                StringBuffer content = new StringBuffer();

                while ((inputLine = in.readLine()) != null) {
                    content.append(inputLine);
                }

                for (Object o : new JSONObject(content.toString()).optJSONObject("analytics.cursor").getJSONArray("data")) {

                    JSONArray test = (JSONArray) o;
                    double tupleCount = test.optDouble(0);
                    nextLoadDate = test.optString(2);

                    // На нулевой итерации (iterationCount = 0) обрабатывается дата, данные за которую были загружены на предыдущем этапе загрузки.
                    // В ходе предыдущего запуска
                    // Если tupleCount = 0, то данных на текущую дату (currentLoadDate) нет.
                    if (iterationCount > 0 && tupleCount > 0) {
                        ImoexMetaData imoexMetaData = new ImoexMetaData();
                        imoexMetaData.pageSize = test.optInt(1);
                        imoexMetaData.pageCount = (int) Math.ceil(tupleCount / test.optDouble(1));
                        imoexMetaData.loadDate = currentLoadDate;
                        imoexMetaDataArray.add(imoexMetaData);
                    }
                }

            }
            conn.disconnect();
        }
    }

    private void loadData(PreparedStatement stmtUpdate, String urlString) throws Exception {
//        String urlString = "http://iss.moex.com/iss/statistics/engines/stock/markets/index/analytics/IMOEX.json?iss.meta=off&iss.only=analytics&analytics.columns=indexid,tradedate,secids,weight";

        for (ImoexMetaData imoexMetaData : imoexMetaDataArray) {
//            System.out.println(imoexMetaData.loadDate);
            for (int ind = 0; ind < imoexMetaData.pageCount; ind++) {
                URL url = new URL(urlString.concat("&date=" + imoexMetaData.loadDate).concat("&start=" + String.valueOf(ind * imoexMetaData.pageSize)));
//                System.out.println(url);
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

                    for (Object o : new JSONObject(content.toString()).optJSONObject("analytics").getJSONArray("data")) {
                        JSONArray test = (JSONArray) o;
                        imoexDataArray.add(accumulateData(test));
//                        indexSecurityArray.add(new StageIndexSecurityWeight.IndexSecurityData(test.optString(0), test.optString(1), test.optString(2), test.optDouble(3)));
                    }
                }
                conn.disconnect();
            }
            // В буфере одновременно Храним не более 50_000 записей
            if (imoexDataArray.size() >= 50_000) {
                saveData(stmtUpdate, imoexDataArray);
                imoexDataArray.clear();
            }
        }

        saveData(stmtUpdate, imoexDataArray);
        imoexDataArray.clear();

    }


    void completeLoad(PreparedStatement stmtUpdate, String urlStringMeta, String urlStringData) throws Exception {
        System.out.println("------ " + imoexMetaDataArray.size());
        loadMetaData(urlStringMeta);
        loadData(stmtUpdate, urlStringData);
    }
    abstract <T extends ImoexData> T accumulateData(JSONArray jsonArray);
    abstract void saveData(PreparedStatement stmtUpdate, List<? extends ImoexData> dataArray);
}
