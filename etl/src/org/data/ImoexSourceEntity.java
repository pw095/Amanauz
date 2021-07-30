package org.data;

import org.json.JSONArray;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

import static org.util.AuxUtil.dateFormat;

public interface ImoexSourceEntity extends StageEntity {
    public abstract <T extends ExternalData> T accumulateData(JSONArray jsonArray);

    default public void postLoad(PreparedStatement stmtUpdate, List<? extends ExternalData> dataArray) {
        System.out.println("savePortion");
        saveData(stmtUpdate, dataArray);
        try {
            stmtUpdate.getConnection().commit();
        } catch (SQLException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
        dataArray.clear();
    }
    default public void load(PreparedStatement stmtUpdate, String urlString, String objectJSON) {
        List<ExternalData> imoexDataArray = new ArrayList<>();
        imoexDataArray.addAll(coreLoad(urlString, objectJSON));
        postLoad(stmtUpdate, imoexDataArray);
    }
    default public List<? extends ExternalData> coreLoad(String urlString, String objectJSON) {
        List<ExternalData> imoexDataArray = new ArrayList<>();
        try {
            System.out.println(urlString);
            URL url = new URL(urlString);
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("GET");

            conn.getResponseCode();
            try (BufferedReader in = new BufferedReader(new InputStreamReader(conn.getInputStream()))) {
                JSONArray arr = null;
                String inputLine;
                StringBuffer content = new StringBuffer();
                while ((inputLine = in.readLine()) != null) {
                    content.append(inputLine);
                }
                try {
                    arr = new JSONObject(content.toString()).optJSONObject(objectJSON).getJSONArray("data");
                } catch (NullPointerException e) {
                    e.printStackTrace();
                }
                for (Object o : arr) {
                    JSONArray test = (JSONArray) o;
                    imoexDataArray.add(accumulateData(test));
                }
            }
            conn.disconnect();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return imoexDataArray;
    }

    default public void multiLoad(PreparedStatement stmtUpdate, String urlString, String objectJSON) {
        List<ExternalData> imoexDataArray = new ArrayList<>();
        int pageSize = 100;
        boolean exitFlag=true;
        for (int ind = 0; ind < 500 && exitFlag; ++ind) {
            exitFlag = imoexDataArray.addAll(coreLoad(urlString + "&start=" + ind * pageSize, objectJSON));
            if (imoexDataArray.size() > 10_000) {
                postLoad(stmtUpdate, imoexDataArray);
            }
        }
        postLoad(stmtUpdate, imoexDataArray);
    }

    default public List<? extends ExternalData> periodMultiCoreLoad(String urlString, String objectJSON) {
        List<ExternalData> imoexDataArray = new ArrayList<>();
        int pageSize = 100;
        boolean exitFlag=true;
        for (int ind = 0; ind < 500 && exitFlag; ++ind) {
            exitFlag = imoexDataArray.addAll(coreLoad(urlString + "&start=" + ind * pageSize, objectJSON));
        }
        return imoexDataArray;
    }

    default public void periodMultiLoad(PreparedStatement stmtUpdate, String urlString, PeriodEntity entity, String objectJSON) {
        List<ExternalData> imoexDataArray = new ArrayList<>();
        for(LocalDate currentDate = LocalDate.parse(entity.getEffectiveFromDt(), dateFormat);
            !currentDate.isAfter(LocalDate.parse(entity.getEffectiveToDt(), dateFormat));
            currentDate = currentDate.plusDays(1)) {
            imoexDataArray.addAll(periodMultiCoreLoad(urlString.concat("&date=").concat(currentDate.format(dateFormat)), objectJSON));
            if (imoexDataArray.size() > 10_000) {
                postLoad(stmtUpdate, imoexDataArray);
            }
        }
        postLoad(stmtUpdate, imoexDataArray);
    }
}
