package org.data;

import org.json.JSONArray;
import org.json.JSONObject;
import org.meta.LoadMode;
import org.meta.MetaLayer;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
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

    /*private static class ImoexMetaData {
        String loadDate;
        int pageSize;
        int pageCount;
    }*/
    static abstract class ImoexData {}

//    private List<ImoexMetaData> imoexMetaDataArray = new ArrayList<>();
    private List<ImoexData> imoexDataArray = new ArrayList<>();
    String previousEffectiveToDt;
    private String effectiveFromDt;
    private String effectiveToDt;

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

    private int pageSize=100;
    private int pageCount;

    public ImoexWebSiteEntity(long flowLoadId, String entityCode) {
        super(flowLoadId, MetaLayer.STAGE, entityCode);
    }

    public void historyLoad(PreparedStatement stmtUpdate, String urlString, String objectJSON) throws Exception {
        for(LocalDate currentDate = LocalDate.parse(this.getEffectiveFromDt(), dateFormat); !currentDate.isAfter(LocalDate.now()); currentDate = currentDate.plusDays(1)) {
            multipleIterationsLoad(stmtUpdate, urlString.concat("&date=").concat(currentDate.format(dateFormat)), objectJSON);
            this.setEffectiveToDt(currentDate.format(dateFormat));
        }
    }

    public void noHistoryLoad(PreparedStatement stmtUpdate, String urlString, String objectJSON, boolean multipleFlag) throws Exception {
        if (multipleFlag) {
            multipleIterationsLoad(stmtUpdate, urlString, objectJSON);
        } else {
            singleIterationLoad(stmtUpdate, urlString, objectJSON);
        }
        this.setEffectiveFromDt(LocalDate.of(1900, 01, 01).format(dateFormat));
        this.setEffectiveToDt(LocalDate.of(2999, 12, 31).format(dateFormat));
    }

    public void multipleIterationsLoad(PreparedStatement stmtUpdate, String urlString, String objectJSON) throws Exception {
        boolean exitFlag = true;
        for (int ind = 0; ind < 500 && exitFlag; ++ind) {
            exitFlag = coreLoad(urlString.concat("&start=" + String.valueOf(ind * pageSize)), objectJSON);
            // В буфере одновременно Храним не более 50_000 записей
            if (imoexDataArray.size() >= 50_000) {
                saveData(stmtUpdate, imoexDataArray);
                imoexDataArray.clear();
            }
        }
        saveData(stmtUpdate, imoexDataArray);
        imoexDataArray.clear();
    }

    public void singleIterationLoad(PreparedStatement stmtUpdate, String urlString, String objectJSON) throws Exception {
        coreLoad(urlString, objectJSON);
        saveData(stmtUpdate, imoexDataArray);
        imoexDataArray.clear();
    }

    public boolean coreLoad(String urlString, String objectJSON) throws Exception {
        boolean exitFlag = true;
        URL url = new URL(urlString);
        System.out.println(url);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("GET");

        conn.getResponseCode();
        try (BufferedReader in = new BufferedReader(new InputStreamReader(conn.getInputStream()))) {
            Map<String, Double> inMap = new HashMap<>();
            String inputLine;
            StringBuffer content = new StringBuffer();

            while ((inputLine = in.readLine()) != null) {
                content.append(inputLine);
            }

            JSONArray arr = null;

            try {
                arr = new JSONObject(content.toString()).optJSONObject(objectJSON).getJSONArray("data");
            } catch (NullPointerException e) {}
            exitFlag = arr.length() == 0 ? false : true;
            for (Object o : arr) {
                JSONArray test = (JSONArray) o;
                imoexDataArray.add(accumulateData(test));
            }
        }
        conn.disconnect();
        return exitFlag;
    }


/*        for(int ind=0; ind < 500 && exitFlag; ++ind) {
            URL url = new URL(urlString.concat("&start=" + String.valueOf(ind * pageSize)));
            System.out.println(url);
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("GET");

            int status = conn.getResponseCode();
            try (BufferedReader in = new BufferedReader(new InputStreamReader(conn.getInputStream()))) {
                String inputLine;
                StringBuffer content = new StringBuffer();

                while ((inputLine = in.readLine()) != null) {
                    content.append(inputLine);
                }

                JSONArray arr = null;
                try {
                    arr = new JSONObject(content.toString()).optJSONObject(objectJSON).getJSONArray("data");
                } catch (NullPointerException e) {}

                exitFlag = arr.length() == 0 ? false : true;
                for (Object o : arr) {
                    JSONArray test = (JSONArray) o;
                    imoexDataArray.add(accumulateData(test));
                }
            }
            conn.disconnect();

            // В буфере одновременно Храним не более 50_000 записей
            if (imoexDataArray.size() >= 50_000) {
                saveData(stmtUpdate, imoexDataArray);
                imoexDataArray.clear();
            }
        }
        saveData(stmtUpdate, imoexDataArray);
        imoexDataArray.clear();
    }*/
/*
    private void loadData(PreparedStatement stmtUpdate, String urlString) throws Exception {
//        String urlString = "http://iss.moex.com/iss/statistics/engines/stock/markets/index/analytics/IMOEX.json?iss.meta=off&iss.only=analytics&analytics.columns=indexid,tradedate,secids,weight";

        for(int ind=0; ind < 500; ++ind) {
            URL url = new URL(urlString.concat("&start=" + String.valueOf(ind * pageSize)));
            System.out.println(url);
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

                JSONArray arr = null;
                try {
                    arr = new JSONObject(content.toString()).optJSONObject("analytics").getJSONArray("data");
                } catch (NullPointerException e) {}
                try {
                    arr = new JSONObject(content.toString()).optJSONObject("history").getJSONArray("data");
                } catch (NullPointerException e) {}
                try {
                    arr = new JSONObject(content.toString()).optJSONObject("securities").getJSONArray("data");
                } catch (NullPointerException e) {}
                if (arr.length() == 0) {
                    break;
                }
                for (Object o : arr) {
                    JSONArray test = (JSONArray) o;
                    imoexDataArray.add(accumulateData(test));
//                        indexSecurityArray.add(new StageIndexSecurityWeight.IndexSecurityData(test.optString(0), test.optString(1), test.optString(2), test.optDouble(3)));
                }
            }
            conn.disconnect();
            // В буфере одновременно Храним не более 50_000 записей
            if (imoexDataArray.size() >= 50_000) {
                saveData(stmtUpdate, imoexDataArray);
                imoexDataArray.clear();
            }
        }
        saveData(stmtUpdate, imoexDataArray);
        imoexDataArray.clear();
        */
/*
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

                    JSONArray arr = null;
                    try {
                        arr = new JSONObject(content.toString()).optJSONObject("analytics").getJSONArray("data");
                    } catch (NullPointerException e) {}
                    try {
                        arr = new JSONObject(content.toString()).optJSONObject("history").getJSONArray("data");
                    } catch (NullPointerException e) {}
                    try {
                        arr = new JSONObject(content.toString()).optJSONObject("securities").getJSONArray("data");
                    } catch (NullPointerException e) {}
                    for (Object o : arr) {
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
*/



/*    void completeLoad(PreparedStatement stmtUpdate, String urlStringMeta, String urlStringData) throws Exception {

        if (urlStringMeta != null) {
            loadMetaData(urlStringMeta);
            System.out.println("------ " + imoexMetaDataArray.size());
            System.out.println(imoexMetaDataArray.get(0).loadDate);
        }

        loadData(stmtUpdate, urlStringData);
    }*/
    abstract <T extends ImoexData> T accumulateData(JSONArray jsonArray);
    abstract void saveData(PreparedStatement stmtUpdate, List<? extends ImoexData> dataArray);
}
